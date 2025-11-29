import os
import socket
import logging
import threading
from common.processor import TransactionCSVProcessor
from common.protocol import Protocol
from common.new_protocolo import ProtocolNew
from common.graceful_shutdown import GracefulShutdown  

logger = logging.getLogger(__name__)

class Client:
    def __init__(self, server_port, max_batch_size, client_id):
        self.shutdown = GracefulShutdown()
        self.shutdown.register_callback(self._on_shutdown_signal)
        
        self.server_port = server_port
        self.reports_port = server_port + 1
        self.max_batch_size = int(max_batch_size)
        self.client_id = client_id
        self.keep_running = False
        self.client_socket = None
        self.protocol = None  
        self.processor = None
        self.expected_reports = 6
        
        self.report_files = {} 
        self.report_headers = {
        'q1': 'transaction_id,final_amount',
        'q2_most_profit': 'year_month_created_at,item_name,profit_sum',
        'q2_best_selling': 'year_month_created_at,item_name,sellings_qty',
        'q3': 'year_half_created_at,store_name,tpv',
        'q4': 'store_name,birthdate'
        }
        

    def _on_shutdown_signal(self):
        logger.info("Señal de shutdown recibida en Client")
        self.keep_running = False
        if self.client_socket:
            try:
                self.client_socket.close()
            except:
                pass     
                
    def run(self):
        self.keep_running = True
        try:
            if self.shutdown.is_shutting_down():
                logger.info("Shutdown detectado, no conectando")
                return
            
            logger.info(f"Conectando al servidor en el puerto {self.server_port}")
            self.client_socket = socket.create_connection(('gateway', self.server_port))
            
            self.protocol = ProtocolNew(self.client_socket)
            
            self.receiver_thread = threading.Thread(target=self.receive_reports, daemon=False)
            self.receiver_thread.start()

            self.process_and_send_files_from_volumes()

        except socket.error as err:
            logger.error(f"Error de socket: {err}")
        except Exception as err:
            logger.error(f"Error inesperado: {err}")
        # finally:
        #     self._cleanup()
    
    def process_and_send_files_from_volumes(self):
        mounted_folders = {
            #"D": "/data/transactions",
            "D": "/data/transactions_test",
            #"I": "/data/transaction_items",
             "I": "/data/transactions_items_test",
            "U": "/data/users",
            "S": "/data/stores",
            "M": "/data/menu_items",
            #"payment_methods": "/data/payment_methods",
            #"vouchers": "/data/vouchers"
        }

        try:
            for file_type, folder_path in mounted_folders.items():                
                if self.shutdown.is_shutting_down():
                    logger.info("Shutdown detectado, deteniendo envío de archivos")
                    break
                
                if not os.path.exists(folder_path):
                    logging.warning(f"La carpeta {folder_path} no existe. Saltando...")
                    continue
                files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
                if not files:
                    logging.info(f"No se encontraron archivos en {folder_path}.")
                    continue

                logging.info(f"Procesando archivos en {folder_path}: {files}")

                for file in files:
                    if self.shutdown.is_shutting_down():
                        logger.info("Shutdown detectado durante procesamiento")
                        break
                    
                    file_path = os.path.join(folder_path, file)
                    self.send_data(file_path, file_type)
                
                if not self.shutdown.is_shutting_down():
                    self.protocol.send_finish_message(file_type)
                                
            if self.protocol and not self.shutdown.is_shutting_down():
                self.protocol.send_exit_message()
                #self.receive_reports()

        except Exception as e:
            logger.error(f"Error procesando archivos desde volúmenes: {e}")
            raise
    
    def send_data(self, csv_filepath, file_type):
        """Procesa y envía un archivo CSV línea por línea."""
        try:
            logger.info(f"Iniciando procesamiento de {csv_filepath} con file_type {file_type}")
            self.processor = TransactionCSVProcessor(csv_filepath, self.max_batch_size)
            
            while self.processor.has_more_batches() and self.keep_running:
                if self.shutdown.is_shutting_down():
                    logger.info("Shutdown detectado, deteniendo envío de batches")
                    break
                
                batch_result = self.processor.read_next_batch()
                
                if not batch_result.items:
                    logger.info("Batch vacío, terminando...")
                    break
                
                success = self.protocol.send_batch_message(batch_result, file_type)
                
                if not success:
                    logger.error("Error enviando batch o no se recibió ACK. Deteniendo el envío.")
                    break
            
            logger.info(f"Procesamiento completado para {csv_filepath}")
            
        except Exception as e:
            logger.error(f"Error en send_data: {e}")
            raise
    
    def receive_reports(self):
        """Recibe reportes del servidor usando el sistema de mensajes."""
        try:
            if not self.protocol:
                logger.error("Protocolo no inicializado. No se pueden recibir reportes.")
                return
            
            logger.info("Esperando reportes del servidor...")
            
            # total_messages_received = 0
            # total_lines_received = 0
            
            for message in self.protocol.receive_reports():
                if self.shutdown.is_shutting_down():
                    logger.info("Shutdown detectado, deteniendo recepción")
                    break
                
                if message.action == "L":
                    # lines_in_message = len([l for l in message.data.strip().split('\n') if l.strip()])
                    # total_messages_received += 1
                    # total_lines_received += lines_in_message
                    
                    self._process_report_data(message.file_type, message.data)
                    
                elif message.action == "EXIT":
                    logger.info("EXIT recibido, cerrando archivos de reportes")
                    #logger.info(f"TOTAL RECIBIDO: {total_messages_received} mensajes, {total_lines_received} líneas")
                    self._close_all_report_files()
                    break
                
                else:
                    logger.warning(f"Mensaje inesperado: {message.action}")
            
            #logger.info(f"Recepción completada: {total_messages_received} mensajes, {total_lines_received} líneas")
            
        except Exception as e:
            logger.error(f"Error recibiendo reportes: {e}")
            raise
        finally:
            self._cleanup()
        
    def _process_report_data(self, query_name, data):
        try:
            if query_name not in self.report_files:
                self._open_report_file(query_name)
            
            file_handle = self.report_files[query_name]
            file_handle.write(data)
            if not data.endswith('\n'):
                file_handle.write('\n')
            
            #lines_count = data.count('\n') + (1 if data and not data.endswith('\n') else 0)
            #logger.info(f"Datos de {query_name} escritos: {lines_count} líneas")
            file_handle.flush()
            
        except Exception as e:
            logger.error(f"Error procesando datos de {query_name}: {e}")

    def _open_report_file(self, query_name):
        try:
            report_dir = f"/app/report_{self.client_id}"
            os.makedirs(report_dir, exist_ok=True)
            
            filename = f"{report_dir}/report_{query_name.lower()}.csv"
            file_handle = open(filename, 'w')
            
            if query_name in self.report_headers:
                file_handle.write(self.report_headers[query_name] + '\n')
            
            self.report_files[query_name] = file_handle
            logger.info(f"Archivo {filename} abierto para {query_name}")
            
        except Exception as e:
            logger.error(f"Error abriendo archivo para {query_name}: {e}")

    def _close_all_report_files(self):
        try:
            for query_name, file_handle in self.report_files.items():
                try:
                    file_handle.close()
                    logger.info(f"Archivo de {query_name} cerrado")
                except Exception as e:
                    logger.error(f"Error cerrando archivo de {query_name}: {e}")
            
            self.report_files.clear()
            logger.info("Todos los archivos de reportes cerrados")
            
        except Exception as e:
            logger.error(f"Error cerrando archivos de reportes: {e}")

        
    def _cleanup(self):
        """Limpieza de recursos al finalizar."""
        self.keep_running = False
        
        if self.processor:
            self.processor.close()
            logger.info("Procesador CSV cerrado")
        
        if self.protocol:
            self.protocol.close()
        elif self.client_socket:
            self.client_socket.close()
            
        # try:
        #     self.receiver_thread.join(timeout=5.0)
        # except Exception as e:
        #     logger.error(f"Error esperando el hilo receptor: {e}") 
            
        logger.info("Cliente cerrado completamente")
        
        
        
    # def receive_reports(self):
    #     """Recibe reportes del servidor hasta que se envíe un mensaje de salida."""
    #     try:
    #         if not self.protocol:
    #             logger.error("Protocolo no inicializado. No se pueden recibir reportes.")
    #             return
            
    #         logger.info("Esperando reportes del servidor...")
            
    #         # Esperamos recibir 3 reportes: Q1, Q3, Q4
    #         reports_received = 0
            
    #         while self.keep_running and reports_received < self.expected_reports:
    #             if self.shutdown.is_shutting_down():
    #                 logger.info("Shutdown detectado, deteniendo recepción de reportes")
    #                 break
                
    #             logger.info(f"Esperando reporte {reports_received + 1} de {self.expected_reports}...")
                
    #             report = self.protocol.receive_report()  # ← Retorna dict
                
    #             if report is None:
    #                 logger.info("No se recibieron más reportes o conexión cerrada.")
    #                 break
                
    #             reports_received += 1
                
    #             # Extraer datos del dict
    #             query_id = report['query_id']
    #             content = report['content']
                
    #             logger.info(f"Reporte Q{query_id} recibido: {len(content)} bytes")
                
    #             # Guardar reporte en archivo usando el content (str)
    #             self._save_report_to_file(f"Q{query_id}", content)
            
    #         logger.info(f"Recepción completada. {reports_received} reportes recibidos.")
            
    #     except Exception as e:
    #         logger.error(f"Error recibiendo reportes: {e}")
    #         raise
    
    # def _save_report_to_file(self, report_type, content):
    #     try:
    #         # Crear directorio report_<client_id> si no existe
    #         report_dir = f"/app/report_{self.client_id}"
    #         os.makedirs(report_dir, exist_ok=True)
            
    #         # Guardar en la carpeta mapeada
    #         filename = f"{report_dir}/report_{report_type.lower()}.csv"
    #         with open(filename, 'w') as f:
    #             f.write(content)
    #         logger.info(f"Reporte {report_type} guardado en {filename}")
    #     except Exception as e:
    #         logger.error(f"Error guardando reporte {report_type}: {e}")