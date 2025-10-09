import logging
import os
import sys
from rabbitmq.middleware import MessageMiddlewareQueue
from strategies import FilterStrategyFactory
from configurators import NodeConfiguratorFactory
from dtos.dto import TransactionBatchDTO, TransactionItemBatchDTO, BatchType, FileType
from common.graceful_shutdown import GracefulShutdown  

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FilterNode:
    def __init__(self):
        self.shutdown = GracefulShutdown()
        self.shutdown.register_callback(self._on_shutdown_signal)
        
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.input_queue = os.getenv('INPUT_QUEUE', None)
        self.output_q1 = os.getenv('OUTPUT_Q1', None)
        self.output_q2 = os.getenv('OUTPUT_Q2', None)
        self.output_q3 = os.getenv('OUTPUT_Q3', None)
        self.output_q4 = os.getenv('OUTPUT_Q4', None)
        self.filter_mode = os.getenv('FILTER_MODE', 'year')
        self.input_exchange = os.getenv('INPUT_EXCHANGE', None)
        
        total_env_var = f'TOTAL_{self.filter_mode.upper()}_FILTERS'
        self.total_filters = int(os.getenv(total_env_var, '1'))
        
        logger.info(f"FilterNode inicializado:")
        logger.info(f"  Modo: {self.filter_mode}")
        logger.info(f"  Queue entrada: {self.input_queue}")
        logger.info(f"  Total filtros {self.filter_mode}: {self.total_filters}")
        
        self.filter_strategy = self._create_filter_strategy()
        
        self.node_configurator = NodeConfiguratorFactory.create_configurator(
            self.filter_mode,
            self.rabbitmq_host
        )
        
        if self.filter_mode == 'year':
            self.input_middleware = self.node_configurator.create_input_middleware(
                self.input_exchange, self.input_queue
            )
        else: 
            self.input_middleware = self.node_configurator.create_input_middleware(
                self.input_queue, ""
            )
        
        if hasattr(self.input_middleware, 'shutdown'):
            self.input_middleware.shutdown = self.shutdown
        
        self.middlewares = self.node_configurator.create_output_middlewares(
            self.output_q1,
            self.output_q3,
            self.output_q4,
            self.output_q2
        )
        
        for name, middleware in self.middlewares.items():
            if middleware and hasattr(middleware, 'shutdown'):
                middleware.shutdown = self.shutdown

    def _on_shutdown_signal(self):
        """Callback ejecutado cuando se recibe señal de shutdown"""
        logger.info("FilterNode: Señal de shutdown recibida, deteniendo consumo...")
        try:
            if self.input_middleware:
                self.input_middleware.stop_consuming()
        except Exception as e:
            logger.error(f"Error deteniendo consumo: {e}")
            
    def _create_filter_strategy(self):
        try:
            config = {}
            
            if self.filter_mode == 'year':
                config['filter_years'] = os.getenv('FILTER_YEARS', '2024,2025')
            elif self.filter_mode == 'hour':
                config['filter_hours'] = os.getenv('FILTER_HOURS', '06:00-23:00')
            elif self.filter_mode == 'amount':
                config['min_amount'] = float(os.getenv('MIN_AMOUNT', '75'))
            
            return FilterStrategyFactory.create_strategy(self.filter_mode, **config)
            
        except Exception as e:
            logger.error(f"Error creando estrategia de filtro: {e}")
            raise



    def process_message(self, body: bytes, routing_key: str = None):
        if self.shutdown.is_shutting_down():
            logger.warning("Shutdown en progreso, ignorando mensaje")
            return True
        
        try:
            should_stop, batch_type, dto, is_eof = self.node_configurator.process_message(
                body, routing_key
            )
            
            if is_eof:
                return self._handle_eof_message(dto, batch_type)
            
            if should_stop:
                return True
                        
            decoded_data = body.decode('utf-8').strip()
            
            if decoded_data.startswith("D:"):
                decoded_data = decoded_data[2:]
            elif decoded_data.startswith("I:"):
                decoded_data = decoded_data[2:]
            
            if hasattr(self.filter_strategy, 'set_dto_helper'):
                self.filter_strategy.set_dto_helper(dto)
            
            filtered_csv = self.filter_strategy.filter_csv_batch(decoded_data)
            
            if not filtered_csv.strip():
                return False
            
            if self.shutdown.is_shutting_down():
                logger.warning("Shutdown en progreso, no se enviarán datos")
                return True
                        
            processed_data = self.node_configurator.process_filtered_data(filtered_csv)
            self.node_configurator.send_data(processed_data, self.middlewares, batch_type)
            
            return False

        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")
            return False
        
    def _handle_eof_message(self, dto: TransactionBatchDTO, eof_type: str):
        try:
            eof_data = dto.data.strip()
            if ":" in eof_data:
                parts = eof_data.split(':')
                counter = int(parts[-1])  
            else:
                counter = 1
            
            logger.info(f"EOF recibido: tipo={eof_type}, counter={counter}, total_filters={self.total_filters}")
            
            should_stop = self.node_configurator.handle_eof(
                counter=counter,
                total_filters=self.total_filters,
                eof_type=eof_type,
                middlewares=self.middlewares,
                input_middleware=self.input_middleware
            )
            
            if should_stop:
                logger.info("Configurador indica que debe cerrarse el nodo")
            
            return should_stop
            
        except Exception as e:
            logger.error(f"Error manejando EOF: {e}")
            return False
        
        
    def on_message_callback(self, ch, method, properties, body):
        try:
            if self.shutdown.is_shutting_down():
                logger.warning("Shutdown solicitado, deteniendo consumo")
                ch.stop_consuming()
                return
            
            routing_key = method.routing_key if hasattr(method, 'routing_key') else None
            should_stop = self.process_message(body, routing_key)
            
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
            logger.info("Filtro detenido manualmente")
        except Exception as e:
            logger.error(f"Error durante el consumo: {e}")
            raise
        finally:
            self._cleanup()

    def _cleanup(self):
        """Limpieza ordenada de recursos"""
        logger.info("Iniciando cleanup del FilterNode...")
        
        try:
            # Cerrar middleware de entrada
            if self.input_middleware:
                self.input_middleware.close()
                logger.info("Middleware de entrada cerrado")
            
            # Cerrar todos los middlewares de salida
            for name, middleware in self.middlewares.items():
                if middleware:
                    try:
                        middleware.close()
                        logger.info(f"Middleware '{name}' cerrado")
                    except Exception as e:
                        logger.error(f"Error cerrando middleware '{name}': {e}")
                
        except Exception as e:
            logger.error(f"Error durante cleanup: {e}")

        logger.info("Cleanup completado")


if __name__ == "__main__":
    try:
        filter_node = FilterNode()
        filter_node.start()
        logger.info("FilterNode terminado exitosamente")
        sys.exit(0)     
    except Exception as e:
        logger.error(f"Error fatal: {e}")
        sys.exit(1)