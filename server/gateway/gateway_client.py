from dataclasses import asdict
import os
import threading
from common.new_protocolo import ProtocolMessage, ProtocolNew
from logger import get_logger
import logging
from rabbitmq.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
from dtos.dto import BatchType, MenuItemBatchDTO, ReportBatchDTO, StoreBatchDTO, TransactionBatchDTO, TransactionItemBatchDTO, UserBatchDTO
from client_routing.client_routing import ClientRouter

logger = get_logger(__name__)

class ClientHandler(threading.Thread):
    def __init__(self, client_socket, client_id, gateway, shutdown_handler=None, rabbitmq_host=None,
                 input_reports=None, total_join_nodes=1):
        super().__init__(daemon=False)
        self.client_socket = client_socket
        self.client_id = client_id
        self.gateway = gateway
        self.shutdown = shutdown_handler
        self.protocol = ProtocolNew(client_socket)
        self.rabbitmq_host = rabbitmq_host
        self.input_reports = input_reports
      
        self._is_running = False
        
        self._output_middleware = gateway.get_output_middleware()
        self._join_middleware = gateway.get_join_middleware()

        self.total_join_nodes = int(os.getenv('TOTAL_JOIN_NODES', 1))

        self.reports_queue_name = f"reports_queue_client_{self.client_id}"
        self.routing_key_pattern = f"client.{self.client_id}.#"
        
        self.reports_exchange_name = f"reports_client_{client_id}"
        self.reports_queue_name = f"reports_queue_client_{client_id}"
        
        self.report_middleware = None
        self._setup_middlewares()
        
        self.report_data = {
            'q1': [],
            'q3': [],
            'q4': [],
            'q2_most_profit': [],
            'q2_best_selling': []
        }
        self.eof_count = 0
        #self.max_expected_reports = 5
        self.max_expected_reports = len(self.report_data)
        
        self.reports_config = [
            ('q1', self._convert_q1_to_csv, "Q1", "transacciones"),
            ('q3', self._convert_q3_to_csv, "Q3", "registros"),
            ('q4', self._convert_q4_to_csv, "Q4", "cumpleanos"),
            ('q2_most_profit', self._convert_q2_most_profit_to_csv, "Q2_MOST_PROFIT", "items"),
            ('q2_best_selling', self._convert_q2_best_selling_to_csv, "Q2_BEST_SELLING", "items")
        ]
        
        self.client_router = ClientRouter(total_join_nodes=total_join_nodes)
        self.assigned_join_node = self.client_router.get_node_for_client(self.client_id)
        
        
        
    def _setup_middlewares(self):
        try:
            self.report_middleware = MessageMiddlewareQueue(
                host=self.rabbitmq_host,
                queue_name=f'reports_queue_client_{self.client_id}',  
                exchange_name=self.input_reports,  
                routing_keys=[f'client.{self.client_id}.#'] 
            )

            if self.shutdown and hasattr(self.report_middleware, 'shutdown'):
                self.report_middleware.shutdown = self.shutdown

            logger.info(f"Cliente {self.client_id}: Infraestructura de reportes creada")

        except Exception as e:
            logger.error(f"Cliente {self.client_id}: Error configurando reportes: {e}")
            raise
            
    def run(self):
        self._is_running = True
        logger.info(f"ClientHandler {self.client_id} iniciado")
        
        try:
            for message in self.protocol.receive_messages():
                if not self._is_running: 
                    logger.info(f"Cliente {self.client_id}: _is_running=False, saliendo")
                    break
                if self.shutdown and self.shutdown.is_shutting_down():
                    logger.info(f"Shutdown detectado, cerrando conexión con cliente {self.client_id}")
                    break
                
                
                if message.action == "EXIT":
                    logger.info(f"EXIT received from client {self.client_id}")
                    self._wait_and_send_report()
                    break
                
                elif message.action == "FINISH": 
                    logger.info(f"FINISH received for file_type: {message.file_type} from client {self.client_id}")
                    self._handle_finish(message.file_type)
                
                elif message.action == "BATCH":
                    self._handle_batch_message(message)
                else:
                    logger.warning(f"Unknown action from client {self.client_id}: {message.action}")
                    
        except Exception as e:
            logger.error(f"Error procesando conexión del cliente {self.client_id}: {e}")
            self._send_error_to_client(f"Error processing connection: {e}")
        finally:
            self._cleanup()      
             
    def _get_shard_for_user(self, user_id: str) -> int:
        normalized_user_id = user_id.rstrip('.0') if user_id.endswith('.0') else user_id
        return int(normalized_user_id) % self.total_join_nodes
    
    def _handle_batch_message(self, message):
        handlers = {
            "D": self.process_type_d_message,
            "S": self.process_type_s_message,
            "U": self.process_type_u_message,
            "I": self.process_type_i_message,
            "M": self.process_type_m_message
        }
        
        handler = handlers.get(message.file_type)
        if handler:
            handler(message)
        else:
            logger.warning(f"Unknown file_type from client {self.client_id}: {message.file_type}")
            
    def _get_routing_key_for_join(self, base_key: str) -> str:
        if base_key in ['stores.data']:
            return None  
        else:
            return self.client_router.get_routing_key(self.client_id, base_key)
            
    def _handle_finish(self, file_type: str):        
        if self.shutdown and self.shutdown.is_shutting_down():
            logger.info("Shutdown activo, no enviando EOF")
            return
               
        try:
            if file_type == "D":
                eof_dto = TransactionBatchDTO("EOF:1", batch_type=BatchType.EOF)
                self._output_middleware.send(eof_dto.to_bytes_fast(), routing_key='transactions', headers={'client_id': self.client_id})
                logger.info("EOF:1 enviado")
                
            elif file_type == "S":
                routing_keys = self.client_router.get_all_routing_keys('stores.data')
                for routing_key in routing_keys:
                    eof_dto = StoreBatchDTO("EOF:1", batch_type=BatchType.EOF)
                    self._join_middleware.send(eof_dto.to_bytes_fast(), routing_key=routing_key, headers={'client_id': self.client_id})
                logger.info(f"EOF stores enviado a {len(routing_keys)} join nodes")

            elif file_type == "U":
                try:
                    for node_id in range(self.total_join_nodes):
                        eof_dto = UserBatchDTO("EOF:1", batch_type=BatchType.EOF)
                        routing_key = f"join_node_{node_id}.users.data"
                        
                        self._join_middleware.send(
                            eof_dto.to_bytes_fast(),
                            routing_key=routing_key,
                            headers={'client_id': self.client_id}
                        )
                    
                    logger.info(f"EOF de users enviado a {self.total_join_nodes} nodos")
                except Exception as e:
                    logger.warning(f"Error enviando EOF para users: {e}")
                
            elif file_type == "I":
                eof_dto = TransactionItemBatchDTO("EOF:1", batch_type=BatchType.EOF)
                self._output_middleware.send(eof_dto.to_bytes_fast(), routing_key='transaction_items', headers={'client_id': self.client_id})
                logger.info("EOF:1 enviado para tipo I (transaction_items)")

            elif file_type == "M":
                eof_dto = MenuItemBatchDTO("EOF:1", batch_type=BatchType.EOF)
                routing_key = self._get_routing_key_for_join('menu_items.data')
                self._join_middleware.send(eof_dto.to_bytes_fast(), routing_key=routing_key, headers={'client_id': self.client_id})
                logger.info("EOF:1 enviado para tipo M (menu_items)")

        except Exception as e:
            logger.error(f"Error manejando FINISH: {e}")

    def process_type_d_message(self, message: ProtocolMessage):
        try:
            dto = TransactionBatchDTO(message.data, BatchType.RAW_CSV)
            self._output_middleware.send(dto.to_bytes_fast(), routing_key='transactions', headers={'client_id': self.client_id})
        except Exception as e:
            logger.error(f"Error procesando mensaje de tipo 'D': {e}")

    def process_type_i_message(self, message: ProtocolMessage):
        try:
            dto = TransactionItemBatchDTO(message.data, BatchType.RAW_CSV)
            self._output_middleware.send(dto.to_bytes_fast(), routing_key='transaction_items', headers={'client_id': self.client_id})
        except Exception as e:
            logger.error(f"Error procesando mensaje de tipo 'I': {e}")
            
    def process_type_s_message(self, message: ProtocolMessage):
        try:
            bytes_data = message.data.encode('utf-8')
            dto = StoreBatchDTO.from_bytes_fast(bytes_data)
            serialized_data = dto.to_bytes_fast()

            routing_keys = self.client_router.get_all_routing_keys('stores.data')
            for routing_key in routing_keys:
                self._join_middleware.send(serialized_data, routing_key=routing_key, headers={'client_id': self.client_id})
            
            logger.info(f"Cliente '{self.client_id}' → Stores enviadas a {len(routing_keys)} join nodes")

            line_count = len([line for line in dto.data.split('\n') if line.strip()])
            
        except Exception as e:
            logger.error(f"Error procesando mensaje de tipo 'S': {e}")
            
    def process_type_u_message(self, message: ProtocolMessage):
        try:
            bytes_data = message.data.encode('utf-8')
            dto = UserBatchDTO.from_bytes_fast(bytes_data)
            
            lines = dto.data.split('\n')
            
            header_line = None
            data_lines = []
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                if line.startswith('user_id'):
                    header_line = line
                else:
                    data_lines.append(line)
            
            total_users = len(data_lines)
            logger.info(f"📨 [USERS] Cliente {self.client_id}: Procesando {total_users} users")
            
            CHUNK_SIZE = 5000
            
            for chunk_start in range(0, len(data_lines), CHUNK_SIZE):
                chunk_lines = data_lines[chunk_start:chunk_start + CHUNK_SIZE]
                
                batches_by_node = {i: [] for i in range(self.total_join_nodes)}
                
                for line in chunk_lines:
                    parts = line.split(',')
                    if len(parts) < 1:
                        continue
                    
                    user_id = parts[0]
                    shard_id = self._get_shard_for_user(user_id)
                    batches_by_node[shard_id].append(line)
                
                for node_id, node_lines in batches_by_node.items():
                    if not node_lines:
                        continue
                    
                    lines_to_send = node_lines.copy()
                    
                    if header_line:
                        lines_to_send.insert(0, header_line)
                    
                    batch_data = '\n'.join(lines_to_send)
                    node_dto = UserBatchDTO(batch_data, BatchType.RAW_CSV)
                    
                    routing_key = f"join_node_{node_id}.users.data"
                    
                    self._join_middleware.send(
                        node_dto.to_bytes_fast(),
                        routing_key=routing_key,
                        headers={'client_id': self.client_id}
                    )
                    
                    logger.debug(f"Chunk {chunk_start//CHUNK_SIZE + 1}: Users → join_node_{node_id}: {len(lines_to_send)-1} líneas")
                
                if hasattr(self._join_middleware, 'connection') and self._join_middleware.connection:
                    try:
                        self._join_middleware.connection.process_data_events(time_limit=0)
                    except Exception:
                        pass
            
            logger.info(f"✓ [USERS] Cliente {self.client_id}: {total_users} users enviados en {(len(data_lines) // CHUNK_SIZE) + 1} chunks")
            
        except Exception as e:
            logger.error(f"Error procesando mensaje de tipo 'U': {e}")
            
    def process_type_m_message(self, message: ProtocolMessage):
        try:
            bytes_data = message.data.encode('utf-8')
            dto = MenuItemBatchDTO.from_bytes_fast(bytes_data)
            serialized_data = dto.to_bytes_fast()

            routing_key = self._get_routing_key_for_join('menu_items.data')
            self._join_middleware.send(serialized_data, routing_key=routing_key, headers={'client_id': self.client_id})


            line_count = len([line for line in dto.data.split('\n') if line.strip()])

        except Exception as e:
            logger.error(f"Error procesando mensaje de tipo 'M': {e}")


    def _wait_and_send_report(self):
        if self.shutdown and self.shutdown.is_shutting_down():
            logger.info("Shutdown activo, no esperando reportes")
            return
        
        try:
            logger.info("Esperando reportes del pipeline...")
            
            report_data = self._collect_reports_from_pipeline()
            
            self._send_reports_to_client(report_data)
            self.report_middleware.close()
            
        except Exception as e:
            logger.error(f"Error esperando reportes: {e}")
            self._send_error_to_client(f"Error processing reports: {e}")

    def _collect_reports_from_pipeline(self):
        eof_count = 0
        
        def report_callback(ch, method, properties, body):
            nonlocal eof_count
           
            if self.shutdown and self.shutdown.is_shutting_down():
                logger.info("Shutdown detectado, deteniendo recepción de reportes")
                ch.stop_consuming()
                return    
                    
            try:
                dto = ReportBatchDTO.from_bytes_fast(body)
                routing_key = method.routing_key
                # query_name = routing_key.split('.')[0]
                parts = routing_key.split('.')
                query_name = parts[2] if len(parts) >= 3 else parts[0] 
                
                logger.info(f"Recibido mensaje para {query_name}: {dto.batch_type}, routing: {routing_key}")

                if dto.batch_type == BatchType.EOF:
                    eof_count += 1
                    logger.info(f"EOF recibido para {query_name}. Total EOF: {eof_count}")
                    
                    
                    if eof_count >= self.max_expected_reports:
                        logger.info("Todos los reportes recibidos completamente")
                        ch.stop_consuming()
                    return
                
                if dto.batch_type == BatchType.RAW_CSV:
                    self._process_report_batch(dto.data, query_name, self.report_data)
                    #logger.info(f"Batch procesado: Q1={len(report_data['q1'])}, Q3={len(report_data['q3'])}, Q4={len(report_data['q4'])}")
                    
            except Exception as e:
                logger.error(f"Error procesando batch del reporte: {e}")
        
        self.report_middleware.start_consuming(report_callback)
        return self.report_data

    def _process_report_batch(self, data, query_name, report_data):
        lines = data.strip().split('\n')
        
        for line in lines:
            if not line.strip():
                continue
                
            values = line.split(',')
            
            if query_name == "q1" and len(values) >= 2:
                report_data['q1'].append({
                    "transaction_id": values[0],
                    "final_amount": values[1]
                })
            elif query_name == "q3" and len(values) >= 3:
                report_data['q3'].append({
                    "year_half": values[0],
                    "store_name": values[1],
                    "tpv": values[2]
                })
            elif query_name == "q4" and len(values) >= 2:
                report_data['q4'].append({
                    "store_name": values[0],
                    "birthdate": values[1],
                })
            elif query_name == "q2_most_profit" and len(values) >= 3:
                report_data['q2_most_profit'].append({
                    "year_month_created_at": values[0],
                    "item_name": values[1],
                    "profit_sum": values[2]
                })
            elif query_name == "q2_best_selling" and len(values) >= 3:
                report_data['q2_best_selling'].append({
                    "year_month_created_at": values[0],
                    "item_name": values[1],
                    "sellings_qty": values[2]
                })

    def _send_reports_to_client(self, report_data):

        for query_key, converter_func, report_name, unit_name in self.reports_config:
            transactions = report_data[query_key]
          
            if self.shutdown and self.shutdown.is_shutting_down():
                logger.info("Shutdown detectado, deteniendo envío de reportes")
                break
              
            if transactions:
                csv_content = converter_func(transactions)
                self._send_report_via_protocol(csv_content, report_name)
                logger.info(f"Reporte {report_name} enviado: {len(transactions)} {unit_name}")
            else:
                logger.warning(f"No se encontraron datos para {report_name}")
            
    def _convert_q1_to_csv(self, transactions):
        try:
            csv_lines = []
            for transaction in transactions:
                csv_lines.append(f"{transaction['transaction_id']},{transaction['final_amount']}")
            
            return '\n'.join(csv_lines)
        except Exception as e:
            logger.error(f"Error convirtiendo transacciones a CSV: {e}")
            return "ERROR,0"

    def _convert_q3_to_csv(self, records):
        """Convierte registros Q3 a formato CSV SIN HEADERS."""
        try:
            csv_lines = []
            for record in records:
                csv_lines.append(f"{record['year_half']},{record['store_name']},{record['tpv']}")
            return '\n'.join(csv_lines)
        except Exception as e:
            logger.error(f"Error convirtiendo Q3 a CSV: {e}")
            return "ERROR,ERROR,0"

    def _convert_q4_to_csv(self, records):
        """Convierte registros Q4 a formato CSV SIN HEADERS."""
        try:
            csv_lines = []
            for record in records:
                csv_lines.append(f"{record['store_name']},{record['birthdate']}")
            return '\n'.join(csv_lines)
        except Exception as e:
            logger.error(f"Error convirtiendo Q4 a CSV: {e}")
            return "ERROR,0,0"
    def _convert_q2_most_profit_to_csv(self, records):
        try:
            csv_lines = []
            for record in records:
                csv_lines.append(f"{record['year_month_created_at']},{record['item_name']},{record['profit_sum']}")
            return '\n'.join(csv_lines)
        except Exception as e:
            logger.error(f"Error convirtiendo Q2 Most Profit a CSV: {e}")
            return "ERROR,ERROR,0"

    def _convert_q2_best_selling_to_csv(self, records):
        try:
            csv_lines = []
            logger.info(f"Convirtiendo {len(records)} registros de Q2 Best Selling a CSV")
            for record in records:
                logger.info(f"Procesando registro: {record}")
                csv_lines.append(f"{record['year_month_created_at']},{record['item_name']},{record['sellings_qty']}")
            return '\n'.join(csv_lines)
        except Exception as e:
            logger.error(f"Error convirtiendo Q2 Best Selling a CSV: {e}")
            return "ERROR,ERROR,0"

    def _send_report_via_protocol(self, csv_content, report_name="RPRT"):
        try:
            success = self.protocol.send_response_batches(f"RPRT_{report_name}", "R", csv_content)
            if success:
                logger.info(f"Reporte {report_name} enviado exitosamente: {len(csv_content)} bytes")
            else:
                logger.error(f"Error enviando reporte {report_name}")
        except Exception as e:
            logger.error(f"Error enviando reporte {report_name}: {e}")

    def _send_error_to_client(self, error_message):
        try:
            self.protocol.send_response_message("ERRO", "E", error_message)
        except Exception as e:
            logger.error(f"Error enviando mensaje de error: {e}")
                
    def stop(self):
        logger.info(f"Deteniendo ClientHandler {self.client_id}")
        self._is_running = False            

    def close(self):
        try:
            self.protocol.close()
        except Exception as e:
            logger.error(f"Error cerrando protocolo del cliente {self.client_id}: {e}")

    def is_dead(self):
        return not self.is_alive()

    def _cleanup(self):
        logger.info(f"Limpiando ClientHandler {self.client_id}")
        #self._is_running = False
        
        try:
            if hasattr(self, 'report_middleware') and self.report_middleware:
                self.report_middleware.close()
        except Exception as e:
            logger.error(f"Error cerrando report_middleware: {e}")
        
        try:
            self.protocol.close()
        except Exception as e:
            logger.error(f"Error cerrando protocolo: {e}")
                
        try:
            if self.report_middleware:
                self.report_middleware.close()
        except Exception as e:
            logger.error(f"Error cerrando join_middleware: {e}")
        logger.info(f"ClientHandler {self.client_id} finalizado")