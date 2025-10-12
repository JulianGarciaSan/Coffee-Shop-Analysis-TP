import logging
import os
from datetime import datetime
import sys
from rabbitmq.middleware import MessageMiddlewareExchange
from dtos.dto import TransactionBatchDTO, BatchType, ReportBatchDTO
from common.graceful_shutdown import GracefulShutdown

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ReportGenerator:
    def __init__(self):
        self.shutdown = GracefulShutdown()
        self.shutdown.register_callback(self._on_shutdown_signal)
        
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.report_exchange = os.getenv('REPORT_EXCHANGE', 'report.exchange')
        self.reports_exchange = os.getenv('REPORTS_EXCHANGE', 'reports_exchange')

        self.input_middleware = MessageMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name=self.report_exchange,

            route_keys=['q1.data', 'q3.data', 'q3.eof', 'q4.data', 'q4.eof','q2_most_profit.data','q2_best_selling.data']
        )
        
        if hasattr(self.input_middleware, 'shutdown'):
            self.input_middleware.shutdown = self.shutdown
            
        self.publisher = MessageMiddlewareExchange(
                host=self.rabbitmq_host,
                exchange_name=self.reports_exchange,
                route_keys=['q1.data', 'q1.eof', 'q3.data', 'q3.eof', 'q4.data', 'q4.eof','q2_most_profit.data','q2_best_selling.data']
        )
        
        if hasattr(self.publisher, 'shutdown'):
            self.publisher.shutdown = self.shutdown
            
        self.expected_queries = {'q1'
                                 ,'q3'
                                 #,'q4'
                                 #,'q2_most_profit'
                                 #,'q2_best_selling'
                                 }
        self.total_expected = len(self.expected_queries)
        
        self.csv_files = {}  
        self.eof_received = set() 
        
        logger.info(f"ReportGenerator inicializado:")
        logger.info(f"  Exchange: {self.report_exchange}")
        logger.info(f"  Queries soportadas: Q1, Q2,Q3, Q4")
 
    def _on_shutdown_signal(self):
        logger.info("Señal de shutdown recibida en ReportGenerator")
        if self.input_middleware:
            self.input_middleware.stop_consuming()
  
    def process_message(self, message: bytes, routing_key: str):
        if self.shutdown.is_shutting_down():
            logger.warning("Shutdown en progreso, ignorando mensaje")
            return True
        
        try:
            dto = TransactionBatchDTO.from_bytes_fast(message)
            
            query_name = routing_key.split('.')[0]                       
            
            if dto.batch_type == BatchType.EOF:
                logger.info(f"EOF recibido para {query_name}")
                self._close_csv_file(query_name)
                self.eof_received.add(query_name)
                
                logger.info(f"EOF recibidos: {self.eof_received}")
                logger.info(f"Total expected recibidos: {self.total_expected}")

                if len(self.eof_received) >= self.total_expected:
                    if self.eof_received == self.expected_queries:
                        logger.info("Todos los reportes completados: {}".format(self.eof_received))
                        self._publish_reports()
                        return True
                    else:
                        logger.warning(f"EOF co   if dto.batch_type == BatchType.EOF:unt = 3 pero queries incorrectas: {self.eof_received}")
                return False
            
            # if routing_key.endswith('.data'):
            #     if message == b"EOF:1":
            #         logger.info(f"EOF recibido para {query_name}")
            #         self._close_csv_file(query_name)
            #         self.eof_received.add(query_name)
                
            #         if len(self.eof_received) >= 3:
            #             expected_queries = {'q1', 'q3', 'q4'}
            #             if self.eof_received == expected_queries:
            #                 logger.info("Todos los reportes completados (Q1, Q3, Q4)")
            #                 self._publish_reports()
            #                 return False
            #             else:
            #                 logger.warning(f"EOF count = 3 pero queries incorrectas: {self.eof_received}")
            
            if dto.batch_type == BatchType.RAW_CSV:
                self._write_to_csv(dto.data, query_name)
            
            return False
            
        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")
            return False

    def on_message_callback(self, ch, method, properties, body):
        if self.shutdown.is_shutting_down():
            logger.warning("Shutdown solicitado, deteniendo")
            ch.stop_consuming()
            return

        try:
            routing_key = method.routing_key  
            should_stop = self.process_message(body, routing_key)  

            if should_stop:
                logger.info("Todos los reportes generados - deteniendo consuming")
                ch.stop_consuming()
        except Exception as e:
            logger.error(f"Error en el callback de mensaje: {e}")

    def start(self):
        """Inicia el ReportGenerator."""
        logger.info("Iniciando ReportGenerator...")
        
        try:
            self.input_middleware.start_consuming(self.on_message_callback)
        except KeyboardInterrupt:
            logger.info("ReportGenerator detenido manualmente")
        except Exception as e:
            logger.error(f"Error durante el consumo: {e}")
        finally:
            self._cleanup()

    def _cleanup(self):
        try:
            for query_name in list(self.csv_files.keys()):
                self._close_csv_file(query_name)
            
            if self.input_middleware:
                self.input_middleware.close()
                logger.info("Conexión cerrada")
        except Exception as e:
            logger.error(f"Error durante cleanup: {e}")

    def _initialize_csv_file(self, query_name: str, sample_data: str):
        """Inicializa el archivo CSV con headers."""
        try:
            reports_dir = './reports'
            os.makedirs(reports_dir, exist_ok=True)
            os.chmod(reports_dir, 0o755)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{reports_dir}/{query_name}_{timestamp}.csv"
            
            self.csv_files[query_name] = open(filename, 'w', encoding='utf-8')
            
            header_line = sample_data.strip().split('\n')[0]
            self.csv_files[query_name].write(header_line + '\n')
            self.csv_files[query_name].flush()
            
            logger.info(f"Archivo CSV inicializado: {filename}")

        except Exception as e:
            logger.error(f"Error inicializando el archivo CSV para {query_name}: {e}")
            raise

    def _write_to_csv(self, csv_data: str, query_name: str):
        """Escribe datos al archivo CSV correspondiente."""
        try:
            if query_name not in self.csv_files:
                self._initialize_csv_file(query_name, csv_data)
            
            lines = csv_data.strip().split('\n')
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                if any(header in line.lower() for header in ['transaction_id', 'year_half', 'store_name,birthdate', 'year_month_created_at', 'year_month_created_at', 'sellings_qty', 'profit_sum']):
                    continue
                
                self.csv_files[query_name].write(line + '\n')
            
            self.csv_files[query_name].flush()
            
            # processed_lines = len([l for l in lines if l.strip() and not any(h in l.lower() for h in ['transaction_id', 'year_half', 'store_name,birthdate','sellings_qty','profit_sum'])])
            # if processed_lines > 0:
            #     logger.info(f"Escritas {processed_lines} líneas en archivo {query_name}")
            
        except Exception as e:
            logger.error(f"Error escribiendo en CSV para {query_name}: {e}")
            raise

    def _close_csv_file(self, query_name: str):
        try:
            if query_name in self.csv_files:
                self.csv_files[query_name].close()
                logger.info(f"Archivo CSV cerrado para {query_name}")
                del self.csv_files[query_name]
        except Exception as e:
            logger.error(f"Error cerrando el archivo CSV para {query_name}: {e}")

    def _publish_reports(self):
        try:
            
            batch_size = 150

            for query_name in self.expected_queries:  # Las 3 queries activas

                reports_dir = './reports'
                files = [f for f in os.listdir(reports_dir) if f.startswith(query_name) and f.endswith('.csv')]

                logger.info(f"Publicando reportes para {query_name}, archivos encontrados: {files}")
                for file in files:
                    file_path = os.path.join(reports_dir, file)
                    with open(file_path, 'r', encoding='utf-8') as f:
                        lines = f.readlines()
                        total_lines = len(lines)
                        for i in range(0, total_lines, batch_size):
                            batch_lines = lines[i:i+batch_size]
                            batch_data = ''.join(batch_lines)
                            report_dto = ReportBatchDTO(
                                batch_data,
                                BatchType.RAW_CSV,
                                query_name
                            )
                            self.publisher.send(report_dto.to_bytes_fast(), routing_key=f'{query_name}.data')
                        
                        eof_dto = ReportBatchDTO.create_eof(query_name)
                        self.publisher.send(eof_dto.to_bytes_fast(), routing_key=f'{query_name}.data')
                        logger.info(f"Enviados {total_lines} líneas y EOF para {query_name}")

            logger.info("Todos los reportes publicados exitosamente")
            
        except Exception as e:
            logger.error(f"Error publicando reportes: {e}")

if __name__ == "__main__":
    try:
        report_generator = ReportGenerator()
        report_generator.start()
        logger.info("ReportGenerator terminado exitosamente")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error fatal: {e}")
        sys.exit(1)