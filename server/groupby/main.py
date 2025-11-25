import json
import logging
import os
import sys
from configurators import GroupByConfiguratorFactory
from strategies.groupby_strategy import GroupByStrategyFactory
from dtos.dto import BatchType, TransactionBatchDTO
from common.graceful_shutdown import GracefulShutdown
from healthchecker.healthchecker import HealthChecker
from checkpoint_handler.checkpoint_handler import CheckpointHandler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GroupByNode:
    def __init__(self):
        self.shutdown = GracefulShutdown()
        self.shutdown.register_callback(self._on_shutdown_signal)

        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.groupby_mode = os.getenv('GROUPBY_MODE', 'tpv')
        self.output_exchange = os.getenv('OUTPUT_EXCHANGE', 'join.exchange')
        self.checkpoint_dir = os.getenv('CHECKPOINT_DIR', f'/app/server/logs/groupby_top_customers/checkpoints')

        logger.info(f"GroupByNode inicializado en modo {self.groupby_mode}")
        
        # self.transactions_logger = LoggerMonitor('/app/logs.txt')
        # self.eof_logger = LoggerMonitor('/app/eof_logs.txt')
        self.is_first_message = True 
        
        self.configurator = GroupByConfiguratorFactory.create_configurator(
            self.groupby_mode,
            self.rabbitmq_host,
            self.output_exchange,
        )
        
        strategy_config = self.configurator.get_strategy_config()
        
        self.strategy = GroupByStrategyFactory.create_strategy(
            self.groupby_mode, 
            **strategy_config,
        )
        
        self.checkpoint_handler = CheckpointHandler(
            checkpoint_dir=self.checkpoint_dir,
            strategy=self.strategy,
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
            
        health_port = int(os.getenv('HEALTH_PORT', '9999'))
        self.health_server = HealthChecker(port=health_port)
        self.health_server.start()
            
    def _on_shutdown_signal(self):
        logger.info("Señal de shutdown recibida en GroupByNode")
        if self.input_middleware:
            self.input_middleware.stop_consuming()
    
    def process_message(self, message: bytes, client_id: str = None, message_id: str = None) -> bool:
        if self.shutdown.is_shutting_down():
            return (True, False)
        
        dto = TransactionBatchDTO.from_bytes_fast(message)

        if dto.batch_type == BatchType.EOF:
            self.configurator.handle_eof(dto, self.output_middlewares, self.strategy, client_id, message_id)
            self.checkpoint_handler.save_eof_checkpoint(client_id, message_id)
            return (False, True) 

        if dto.batch_type == BatchType.RAW_CSV:            
            lines = [line.strip() for line in dto.data.split('\n') if line.strip()]
            
            for i, line in enumerate(lines):
                try:
                    self.strategy.process_csv_line(line, client_id)
                except Exception as e:
                    logger.warning(f"Línea {i} inválida en mensaje {message_id}: {e}")
            
            self.checkpoint_handler.save_message_checkpoint(
                client_id=client_id,
                message_id=message_id,
                csv_lines=lines
            )
            
            return (False, True)
        
        return (False, False)
        

        
    def parse_message_headers(self, properties):
        client_id = None
        message_id = None
        if properties and properties.headers:
            client_id = properties.headers.get('client_id')
            message_id = properties.headers.get('message_id')
        return str(client_id), str(message_id)
    
    def on_message_callback(self, ch, method, properties, body):
        try:
            if self.shutdown.is_shutting_down():
                logger.warning("Shutdown solicitado, deteniendo")
                ch.stop_consuming()
                return
            client_id, message_id = self.parse_message_headers(properties)

            if self.checkpoint_handler.analyze_first_message(client_id, message_id, ch, method, body):
                return

            self.checkpoint_handler.register_incoming_message(client_id, message_id)

            should_stop, should_ack = self.process_message(body, client_id, message_id)
        
            if should_ack:
                ch.basic_ack(delivery_tag=method.delivery_tag)
            
            if should_stop:
                logger.info("Shutdown solicitado - deteniendo consuming")
                ch.stop_consuming()
                
        except Exception as e:
            logger.error(f"Error en callback procesando mensaje {client_id}:{message_id}: {e}")
    
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
                    
            if self.health_server:
                self.health_server.stop()
                self.health_server.join(timeout=5.0)
                logger.info("HealthChecker detenido")

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