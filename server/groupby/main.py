import json
import logging
import os
import sys
from configurators import GroupByConfiguratorFactory
from strategies.groupby_strategy import GroupByStrategyFactory
from logger_monitor.logger_monitor import LoggerMonitor
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
        
        self.transactions_logger = LoggerMonitor('/app/logs.txt')
        self.client_logger = LoggerMonitor('/app/client_logs.txt')
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
        
        self.configurator.set_loggers(self.transactions_logger, self.client_logger)
        self.strategy.set_loggers(self.transactions_logger, self.client_logger)
        
        self.checkpoint_handler = CheckpointHandler(
            checkpoint_dir=self.checkpoint_dir,
            node=self.strategy,
            transactions_logger=self.transactions_logger
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
            logger.warning("Shutdown en progreso, ignorando mensaje")
            return (True, False)
        
        dto = TransactionBatchDTO.from_bytes_fast(message)

        if dto.batch_type == BatchType.EOF:
            # client_id_str = str(client_id)
            
            # self.eof_logger.write_with_timestamp(
            #     f"EOF_START:{client_id}:{message_id}"
            # )

            self.configurator.handle_eof(dto, self.output_middlewares, self.strategy, client_id, message_id)

            self.checkpoint_handler.save_eof_checkpoint(client_id, message_id)
            
            # self.eof_logger.write_with_timestamp(f"EOF_COMPLETED:{client_id}")
            return (False, True) 

        if dto.batch_type == BatchType.RAW_CSV:
            
            self.process_csv_line(dto.data, client_id)
            
            self.checkpoint_handler.save_message_checkpoint(client_id, message_id)
            
            return (False, True)
        return (False, False) 
        
    def process_csv_line(self, csv_line: str, client_id: str = 'default_client'):
        for line in csv_line.split('\n'):
                if line.strip():
                    self.strategy.process_csv_line(line.strip(), client_id)
    
    def analyze_first_message(self, client_id: str, message_id: str, ch, method, body) -> bool:
        if self.is_first_message:
            self.is_first_message = False
            
            last_line_client = self.client_logger._get_last_line()
            # last_line_eof = self.eof_logger._get_last_line()
            
            logger.info("=" * 80)
            logger.info("ANALYZE FIRST MESSAGE")
            logger.info(f"   Mensaje actual: {client_id}:{message_id}")
            logger.info(f"   Último del client_log: '{last_line_client}'")
            # logger.info(f"   Último del eof_log: '{last_line_eof}'")
            logger.info("=" * 80)
            
            try:
                self.checkpoint_handler.recover_from_checkpoint()

                if client_id in self.checkpoint_handler.message_trackers:
                    tracker = self.checkpoint_handler.message_trackers[client_id]
                    logger.info(f"Tracker recuperado para cliente {client_id}:")
                    logger.info(f"   Rangos: {tracker.ranges}")
                    
                    try:
                        msg_id_int = int(message_id)
                        in_checkpoint = tracker.contains(msg_id_int)
                        logger.info(f"   ¿Mensaje {msg_id_int} en checkpoint? {in_checkpoint}")
                    except Exception as e:
                        logger.error(f"Error verificando primer mensaje: {e}")
                
            except Exception as e:
                logger.error(f"Error recuperando checkpoint: {e}")
            
            if self._eof_already_processed(client_id):
                logger.info(f"EOF ya procesado - SKIPPING")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return True
            
            logger.info(f"Verificando si mensaje {client_id}:{message_id} está en checkpoint...")
            
            if last_line_client and last_line_client.strip() and ';' in last_line_client:    
                client_id_client_log, message_id_client_log = last_line_client.split(';')
                
                logger.info(f"   Del log: {client_id_client_log}:{message_id_client_log}")
                logger.info(f"   Actual:  {client_id}:{message_id}")
                
                if client_id == client_id_client_log and message_id == message_id_client_log:
                    logger.info(f"COINCIDEN - Verificando checkpoint...")
                    
                    if self._message_in_checkpoint(client_id, message_id):
                        logger.info(f"YA EN CHECKPOINT - SKIPPING y ACK")
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        return True
                    else:
                        logger.warning(f"NO en checkpoint - REPROCESANDO")
                        return False
                else:
                    logger.warning(f"NO COINCIDEN - Mensaje es diferente")
                    return False
            else:
                logger.warning(f"No hay último mensaje válido en log")
                return False
        
        return False

    def _message_in_checkpoint(self, client_id: str, message_id: str) -> bool:
        """
        Verifica si el mensaje específico fue procesado usando rangos.
        """
        if client_id not in self.checkpoint_handler.message_trackers:
            return False
        
        try:
            try:
                msg_id_int = int(message_id)
            except ValueError:
                msg_id_int = int(message_id.split('_')[-1])
            
            is_processed = self.checkpoint_handler.message_trackers[client_id].contains(msg_id_int)
            
            if is_processed:
                logger.debug(f"Mensaje {client_id}:{message_id} encontrado en rangos procesados")
            
            return is_processed
            
        except Exception as e:
            logger.warning(f"Error verificando mensaje en rangos: {e}")
            return False

    def _eof_already_processed(self, client_id: str) -> bool:
        """
        Verifica si el EOF de un cliente ya fue procesado completamente.
        """
        if client_id not in self.checkpoint_handler.message_trackers:
            return False
        
        checkpoint_path = os.path.join(
            self.checkpoint_handler.checkpoint_dir,
            f"client_{client_id}.checkpoint"
        )
        
        if not os.path.exists(checkpoint_path):
            return False
        
        try:
            with open(checkpoint_path, 'r') as f:
                checkpoint_data = json.load(f)
            
            eof_processed = checkpoint_data.get('eof_processed', False)
            
            if eof_processed:
                logger.info(f"EOF de cliente {client_id} ya procesado según checkpoint")
                return True
            
        except Exception as e:
            logger.warning(f"Error leyendo checkpoint para verificar EOF: {e}")
        
        # if last_line_eof and f"EOF_COMPLETED:{client_id}" in last_line_eof:
        #     logger.info(f"EOF de cliente {client_id} ya procesado según log")
        #     return True
        
        return False
    
        
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

            if self.analyze_first_message(client_id, message_id, ch, method, body):
                return
            
            self.client_logger.write(f"{client_id};{message_id}")
            
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