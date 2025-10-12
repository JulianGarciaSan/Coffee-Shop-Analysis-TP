import logging
import os
import sys
from configurators import GroupByConfiguratorFactory
from strategies.groupby_strategy import GroupByStrategyFactory
from dtos.dto import BatchType
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
        
        logger.info(f"GroupByNode inicializado en modo {self.groupby_mode}")
        
        # Crear configurator (maneja middlewares y routing)
        self.configurator = GroupByConfiguratorFactory.create_configurator(
            self.groupby_mode,
            self.rabbitmq_host,
            self.output_exchange
        )
        
        # Crear strategy (maneja lógica de negocio)
        strategy_config = self.configurator.get_strategy_config()
        self.strategy = GroupByStrategyFactory.create_strategy(
            self.groupby_mode, 
            **strategy_config
        )
        
        self.input_middleware = self.configurator.create_input_middleware()
        if hasattr(self.input_middleware, 'shutdown'):
            self.input_middleware.shutdown = self.shutdown
        
        self.output_middlewares = self.configurator.create_output_middlewares()
        for name, middleware in self.output_middlewares.items():
            if middleware and hasattr(middleware, 'shutdown'):
                middleware.shutdown = self.shutdown
        
        if self.groupby_mode in ['top_customers', 'best_selling']:
            self.output_middlewares['input_queue'] = self.input_middleware
   
    def _on_shutdown_signal(self):
        logger.info("Señal de shutdown recibida en GroupByNode")
        if self.input_middleware:
            self.input_middleware.stop_consuming()
    
    def process_message(self, message: bytes, headers: dict = None) -> bool:
        if self.shutdown.is_shutting_down():
            logger.warning("Shutdown en progreso, ignorando mensaje")
            return True
        
        try:
            should_stop, dto, is_eof = self.configurator.process_message(message, headers)
            
            if should_stop:
                return True
            
            if is_eof:
                return self.configurator.handle_eof(dto, self.output_middlewares, self.strategy)
            
            if dto.batch_type == BatchType.RAW_CSV:
                client_id = getattr(dto, 'client_id', 'default_client')
                
                for line in dto.data.split('\n'):
                    if line.strip():
                        # Delegar procesamiento de líneas a la strategy
                        self.strategy.process_csv_line(line.strip(), client_id)
            
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
            
            headers = properties.headers if properties and hasattr(properties, 'headers') else None
            
            should_stop = self.process_message(body, headers)
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
        logger.info("Iniciando cleanup del GroupByNode...")
        
        try:
            if self.input_middleware:
                self.input_middleware.close()
                logger.info("Input middleware cerrado")
            
            if self.output_middlewares:
                for name, middleware in self.output_middlewares.items():
                    middleware.close()
                    logger.info(f"Output middleware '{name}' cerrado")

        except Exception as e:
            logger.error(f"Error en cleanup: {e}")
        
        logger.info("Cleanup completado")


if __name__ == "__main__":
    try:
        node = GroupByNode()
        node.start()
        logger.info("GroupByNode terminado exitosamente")
        sys.exit(0)  
    except Exception as e:
        logger.error(f"Error fatal: {e}")
        sys.exit(1)