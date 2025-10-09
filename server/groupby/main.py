import logging
import os
import sys
from rabbitmq.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
from dtos.dto import TransactionBatchDTO, TransactionItemBatchDTO, BatchType
from groupby_strategy import GroupByStrategyFactory
from common.graceful_shutdown import GracefulShutdown

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GroupByNode:
    def __init__(self):
        self.shutdown = GracefulShutdown()
        self.shutdown.register_callback(self._on_shutdown_signal)

        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.groupby_mode = os.getenv('GROUPBY_MODE', 'tpv')
        self.output_exchange = os.getenv('OUTPUT_EXCHANGE', 'join.exchange')
        
        self.groupby_strategy = self._create_groupby_strategy()
        
        self._setup_input_middleware()
        
        if hasattr(self.input_middleware, 'shutdown'):
            self.input_middleware.shutdown = self.shutdown
        
        self.output_middlewares = self.groupby_strategy.setup_output_middleware(
            self.rabbitmq_host,
            self.output_exchange
        )
        
        for name, middleware in self.output_middlewares.items():
            if middleware and hasattr(middleware, 'shutdown'):
                middleware.shutdown = self.shutdown
        
        if self.groupby_mode in ['top_customers', 'best_selling']:
            self.output_middlewares['input_queue'] = self.input_middleware
        
        logger.info(f"GroupByNode inicializado en modo {self.groupby_mode}")
   
    def _on_shutdown_signal(self):
        logger.info("Señal de shutdown recibida en GroupByNode")
        if self.input_middleware:
            self.input_middleware.stop_consuming()
                
    def _setup_input_middleware(self):
        if self.groupby_mode == 'tpv':
            self.input_exchange = os.getenv('INPUT_EXCHANGE', 'groupby.join.exchange')
            semester = os.getenv('SEMESTER')
            if semester not in ['1', '2']:
                raise ValueError("SEMESTER debe ser '1' o '2'")
            
            self.input_middleware = MessageMiddlewareExchange(
                host=self.rabbitmq_host,
                exchange_name=self.input_exchange,
                route_keys=[f'semester.{semester}', 'eof.all']
            )
            logger.info(f"  Input: Exchange {self.input_exchange}, semestre {semester}")
        
        elif self.groupby_mode == 'top_customers':
            self.input_queue = os.getenv('INPUT_QUEUE', 'year_filtered_q4')
            self.input_middleware = MessageMiddlewareQueue(
                host=self.rabbitmq_host,
                queue_name=self.input_queue
            )
            logger.info(f"  Input: Queue {self.input_queue} (round-robin)")
        
        elif self.groupby_mode == 'best_selling':
            year_routing_key = os.getenv('AGGREGATOR_YEAR', '2024')
            exchange_name = os.getenv('INPUT_EXCHANGE', 'year_filtered_q2')
            queue_name = f"best_selling_{year_routing_key}_queue"
            
            self.input_middleware = MessageMiddlewareQueue(
                host=self.rabbitmq_host,
                queue_name=queue_name,
                exchange_name=exchange_name,
                routing_keys=[year_routing_key]
            )
            logger.info(f"  Input: Queue {queue_name} bindeada a exchange {exchange_name} con routing key {year_routing_key}")
    
    def _create_groupby_strategy(self):
        config = {}
        
        if self.groupby_mode == 'tpv':
            semester = os.getenv('SEMESTER')
            if semester not in ['1', '2']:
                raise ValueError("SEMESTER debe ser '1' o '2'")
            config['semester'] = semester
        
        elif self.groupby_mode == 'top_customers':
            config['input_queue_name'] = os.getenv('INPUT_QUEUE', 'year_filtered_q4')
        
        elif self.groupby_mode == 'best_selling':
            year_routing_key = os.getenv('AGGREGATOR_YEAR', '2024')
            config['input_queue_name'] = f"best_selling_{year_routing_key}_queue"
        
        return GroupByStrategyFactory.create_strategy(self.groupby_mode, **config)
    
    def process_message(self, message: bytes) -> bool:
        if self.shutdown.is_shutting_down():
            logger.warning("Shutdown en progreso, ignorando mensaje")
            return True
        
        try:
            if self.groupby_mode == 'best_selling':
                dto = TransactionItemBatchDTO.from_bytes_fast(message)
            else:
                dto = TransactionBatchDTO.from_bytes_fast(message)
            logger.info(f"Mensaje recibido: batch_type={dto.batch_type}, tamaño={len(dto.data)} bytes")
            if dto.batch_type == BatchType.EOF:
                return self.groupby_strategy.handle_eof_message(dto, self.output_middlewares)
            
            if dto.batch_type == BatchType.RAW_CSV:
                for line in dto.data.split('\n'):
                    if line.strip():
                        self.groupby_strategy.process_csv_line(line.strip())
            
            return False
            
        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")
            return False
    
    def on_message_callback(self, ch, method, properties, body):
        try:
            if self.shutdown.is_shutting_down():
                logger.warning("Shutdown solicitado, deteniendo")
                ch.stop_consuming()
                return
            
            should_stop = self.process_message(body)
            if should_stop:
                logger.info("EOF procesado - deteniendo consuming")
                ch.stop_consuming()
        except Exception as e:
            logger.error(f"Error en callback: {e}")
    
    def start(self):
        try:
            logger.info("Iniciando consumo de mensajes...")
            self.input_middleware.start_consuming(self.on_message_callback)
        except KeyboardInterrupt:
            logger.info("Detenido manualmente")
        finally:
            self._cleanup()
    
    def _cleanup(self):
        try:
            if self.input_middleware:
                self.input_middleware.close()
                logger.info("Input middleware cerrado")
            
            self.groupby_strategy.cleanup_middlewares(self.output_middlewares)
                
        except Exception as e:
            logger.error(f"Error en cleanup: {e}")


if __name__ == "__main__":
    try:
        node = GroupByNode()
        node.start()
        logger.info("GroupByNode terminado exitosamente")
        sys.exit(0)  
    except Exception as e:
        logger.error(f"Error fatal: {e}")
        sys.exit(1)