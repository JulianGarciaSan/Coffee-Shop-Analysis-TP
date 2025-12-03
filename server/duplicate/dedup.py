import logging
import os
import sys
from rabbitmq.middleware import MessageMiddlewareExchange, MessageMiddlewareQueueManual
from common.graceful_shutdown import GracefulShutdown
from healthchecker.healthchecker import HealthChecker
from checkpoint_handler.checkpoint_handler import CheckpointHandler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FilterDuplicateNode:
    def __init__(self):
        self.shutdown = GracefulShutdown()
        self.shutdown.register_callback(self._on_shutdown_signal)
        
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.input_queue = os.getenv('INPUT_QUEUE', 'to_gateway_queue')
        self.output_exchange = os.getenv('OUTPUT_EXCHANGE', 'reports_exchange')
        self.checkpoint_dir = os.getenv('CHECKPOINT_DIR', '/app/dedup/checkpoints')
        
        logger.info(f"FilterDuplicateNode inicializado:")
        logger.info(f"  Input queue: {self.input_queue}")
        logger.info(f"  Output exchange: {self.output_exchange}")
        logger.info(f"  Checkpoint dir: {self.checkpoint_dir}")
        
        self.checkpoint_handler = CheckpointHandler(
            checkpoint_dir=self.checkpoint_dir,
            strategy=self,
            checkpont_interval=1000
        )
        self.input_middleware = MessageMiddlewareQueueManual(
            host=self.rabbitmq_host,
            queue_name=self.input_queue,
            exchange_name='dedup_exchange',
            routing_keys=['q1.data']
        )
        
        if hasattr(self.input_middleware, 'shutdown'):
            self.input_middleware.shutdown = self.shutdown
        
        self.output_middleware = MessageMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name='reports_exchange',
            route_keys=['q1.data']
        )
        
        if hasattr(self.output_middleware, 'shutdown'):
            self.output_middleware.shutdown = self.shutdown
            

        health_port = int(os.getenv('HEALTH_PORT', '9999'))
        self.health_server = HealthChecker(port=health_port)
        self.health_server.start()

    def _on_shutdown_signal(self):
        logger.info("FilterDuplicateNode: Señal de shutdown recibida")
        try:
            if self.input_middleware:
                self.input_middleware.stop_consuming()
        except Exception as e:
            logger.error(f"Error deteniendo consumo: {e}")

    def serialize_operation(self, client_id: str, operation: str) -> str:
        """Serializa una operación (el checkpoint handler lo usa para el WAL)"""
        return f"{client_id}:{operation}"
    
    def deserialize_operation(self, op_str: str) -> dict:
        """Deserializa una operación durante recovery"""
        parts = op_str.split(':', 1)
        if len(parts) == 2:
            return {
                'client_id': parts[0],
                'message_id': parts[1]
            }
        return {}
    
    def apply_operation(self, op_dict: dict):
        """Aplica una operación durante recovery (no hace nada, solo para recovery)"""
        pass
    
    def _serialize_client_data(self, client_id: str) -> dict:
        """Serializa el estado de un cliente (vacío, solo guardamos IDs en tracker)"""
        return {}
    
    def _deserialize_client_data(self, client_id: str, data: dict):
        """Deserializa el estado de un cliente (vacío, solo usamos tracker)"""
        pass

    def is_eof_message(self, body: bytes) -> bool:
        """Verifica si el mensaje es un EOF"""
        try:
            decoded = body.decode('utf-8').strip()
            return decoded.startswith("EOF:")
        except:
            return False

    def process_message(self, body: bytes, client_id: int, message_id: int, is_duplicate: bool = False) -> bool:
        """
        Procesa un mensaje y lo reenvía.
        
        Args:
            body: Cuerpo del mensaje
            client_id: ID del cliente
            message_id: ID del mensaje
            is_duplicate: Si el mensaje ya fue procesado antes
            
        Returns:
            True si se debe detener el consumo, False en caso contrario
        """
        if self.shutdown.is_shutting_down():
            logger.warning("Shutdown en progreso, ignorando mensaje")
            return True
        
        try:
            client_id_str = str(client_id)
            message_id_str = str(message_id)
            
            is_eof = self.is_eof_message(body)
            
            if is_eof:
                self.checkpoint_handler.save_eof_checkpoint(
                    client_id=client_id_str,
                    message_id=message_id_str
                )
                logger.info(f"EOF recibido para cliente {client_id}, reenviando")
            else:
                if not is_duplicate:
                    self.checkpoint_handler.save_message_checkpoint(
                        client_id=client_id_str,
                        message_id=message_id_str,
                        csv_lines=[message_id_str]
                    )
            
            headers = {'client_id': client_id, 'message_id': message_id}
            self.output_middleware.send(body, headers=headers)
            
            logger.debug(f"Mensaje reenviado: {client_id}:{message_id}")
            return False
            
        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")
            return False

    def on_message_callback(self, ch, method, properties, body):
        """Callback para procesar mensajes de RabbitMQ"""
        try:
            if self.shutdown.is_shutting_down():
                logger.warning("Shutdown solicitado, deteniendo consumo")
                ch.stop_consuming()
                return
            
            client_id = None
            message_id = None
            if properties and properties.headers:
                client_id = properties.headers.get('client_id')
                message_id = properties.headers.get('message_id')
            
            if client_id is None or message_id is None:
                logger.error("Mensaje sin client_id o message_id, ignorando")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            client_id_str = str(client_id)
            message_id_str = str(message_id)
            
            if self.checkpoint_handler.is_first_message:
                logger.info("Primer mensaje recibido, ejecutando recovery")
                self.checkpoint_handler.recover_from_checkpoint()
                self.checkpoint_handler.is_first_message = False
            
            msg_id_int = int(message_id)
            is_duplicate = self.checkpoint_handler._is_processed(client_id_str, msg_id_int)
            
            if is_duplicate:
                logger.info(f"Duplicado detectado: {client_id}:{message_id} - reenviando igual")
            
            should_stop = self.process_message(body, client_id, message_id, is_duplicate)
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
            if should_stop:
                logger.info("Deteniendo consumo")
                ch.stop_consuming()
                
        except Exception as e:
            logger.error(f"Error en callback: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def start(self):
        """Inicia el nodo de deduplicación"""
        try:
            logger.info("Iniciando FilterDuplicateNode...")
            logger.info("Recovery se ejecutará con el primer mensaje...")
            
            self.input_middleware.start_consuming(self.on_message_callback)
            
        except KeyboardInterrupt:
            logger.info("FilterDuplicateNode detenido manualmente")
        except Exception as e:
            logger.error(f"Error durante el consumo: {e}")
            raise
        finally:
            self._cleanup()

    def _cleanup(self):
        """Limpieza ordenada de recursos"""
        logger.info("Iniciando cleanup del FilterDuplicateNode...")
        
        try:
            # Cerrar checkpoint handler
            if self.checkpoint_handler:
                self.checkpoint_handler.close()
                logger.info("CheckpointHandler cerrado")
            
            # Cerrar middleware de entrada
            if self.input_middleware:
                self.input_middleware.close()
                logger.info("Input middleware cerrado")
            
            # Cerrar middleware de salida
            if self.output_middleware:
                self.output_middleware.close()
                logger.info("Output middleware cerrado")
            
            # Cerrar health server
            if self.health_server:
                self.health_server.stop()
                self.health_server.join(timeout=5.0)
                logger.info("HealthChecker detenido")
                
        except Exception as e:
            logger.error(f"Error durante cleanup: {e}")
        
        logger.info("Cleanup completado")


if __name__ == "__main__":
    try:
        node = FilterDuplicateNode()
        node.start()
        logger.info("FilterDuplicateNode terminado exitosamente")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error fatal: {e}")
        sys.exit(1)