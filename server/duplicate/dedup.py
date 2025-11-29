# filter_duplicate_node.py
import logging
import os
import sys
from rabbitmq.middleware import MessageMiddlewareExchange, MessageMiddlewareQueueManual
from common.graceful_shutdown import GracefulShutdown
from healthchecker.healthchecker import HealthChecker
from checkpoint_handler.checkpoint_handler import CheckpointHandler
from dedup_strategy import DedupStrategy

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FilterDuplicateNode:
    def __init__(self):
        self.shutdown = GracefulShutdown()
        self.shutdown.register_callback(self._on_shutdown_signal)
        
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.input_queue = os.getenv('INPUT_QUEUE', 'to_gateway_queue')
        self.checkpoint_dir = os.getenv('CHECKPOINT_DIR', '/app/dedup/checkpoints')
        
        logger.info(f"FilterDuplicateNode inicializado:")
        logger.info(f"  Input queue: {self.input_queue}")
        logger.info(f"  Checkpoint dir: {self.checkpoint_dir}")
        
        self.strategy = DedupStrategy()
        
        self.checkpoint_handler = CheckpointHandler(
            checkpoint_dir=self.checkpoint_dir,
            strategy=self.strategy,
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
        logger.info("FilterDuplicateNode: Señal de shutdown recibida, deteniendo consumo...")
        try:
            if self.input_middleware:
                self.input_middleware.stop_consuming()
        except Exception as e:
            logger.error(f"Error deteniendo consumo: {e}")

    def is_eof_message(self, body: bytes) -> bool:
        """
        Verifica si el mensaje es un EOF.
        """
        try:
            decoded = body.decode('utf-8').strip()
            return decoded.startswith("EOF:")
        except:
            return False

    def process_message(self, body: bytes, client_id: int, message_id: int) -> bool:
        """
        Procesa un mensaje y lo reenvía si no es duplicado.
        
        Args:
            body: Cuerpo del mensaje
            client_id: ID del cliente
            message_id: ID del mensaje
            
        Returns:
            True si se debe detener el consumo, False en caso contrario
        """
        if self.shutdown.is_shutting_down():
            logger.warning("Shutdown en progreso, ignorando mensaje")
            return True
        
        try:
            client_id_str = str(client_id)
            
            is_eof = self.is_eof_message(body)
            
            if is_eof:
                if self.strategy.is_eof_received(client_id_str):
                    logger.info(f"EOF duplicado para cliente {client_id}, ignorando")
                    return False
                
                self.strategy.mark_eof(client_id_str)
                
                self.checkpoint_handler.save_eof_checkpoint(
                    client_id=client_id_str,
                    message_id=str(message_id)
                )
                
                logger.info(f"EOF recibido para cliente {client_id}, reenviando")
                
                headers = {'client_id': client_id, 'message_id': message_id}
                self.output_middleware.send(body, headers=headers)
                
                stats = self.strategy.get_stats(client_id_str)
                logger.info(f"Estadísticas finales cliente {client_id}: {stats}")
                
                return False
            
            if self.strategy.is_duplicate(client_id_str, message_id):
                logger.info(f"Mensaje duplicado ignorado: {client_id}:{message_id}")
                return False
            
            self.strategy.mark_as_processed(client_id_str, message_id)
            
            self.checkpoint_handler.save_message_checkpoint(
                client_id=client_id_str,
                message_id=str(message_id),
                csv_lines=[str(message_id)]
            )
            
            headers = {'client_id': client_id, 'message_id': message_id}
            self.output_middleware.send(body, headers=headers)
            
            logger.debug(f"Mensaje reenviado: {client_id}:{message_id}")
            return False
            
        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")
            return False

    def on_message_callback(self, ch, method, properties, body):
        """
        Callback para mensajes recibidos de RabbitMQ.
        """
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
                logger.warning(f"Mensaje sin headers válidos: client_id={client_id}, message_id={message_id}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            if self.checkpoint_handler.analyze_first_message(
                str(client_id), 
                str(message_id), 
                ch, 
                method, 
                body
            ):
                return
            
            should_stop = self.process_message(body, client_id, message_id)
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
            if should_stop:
                logger.info("Shutdown solicitado - deteniendo consuming")
                ch.stop_consuming()
                
        except Exception as e:
            logger.error(f"Error en callback: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def start(self):
        """
        Inicia el consumo de mensajes.
        """
        try:
            logger.info("Iniciando consumo de mensajes en FilterDuplicateNode...")
            
            stats = self.strategy.get_stats()
            logger.info(f"Estadísticas iniciales: {stats}")
            
            self.input_middleware.start_consuming(self.on_message_callback)
            
        except KeyboardInterrupt:
            logger.info("FilterDuplicateNode detenido manualmente")
        except Exception as e:
            logger.error(f"Error durante el consumo: {e}")
            raise
        finally:
            self._cleanup()

    def _cleanup(self):
        """
        Limpieza ordenada de recursos.
        """
        logger.info("Iniciando cleanup del FilterDuplicateNode...")
        
        try:
            stats = self.strategy.get_stats()
            logger.info(f"Estadísticas finales: {stats}")
            
            if self.checkpoint_handler:
                self.checkpoint_handler.close()
                logger.info("CheckpointHandler cerrado")
            
            if self.input_middleware:
                self.input_middleware.close()
                logger.info("Middleware de entrada cerrado")
            
            if self.output_middleware:
                self.output_middleware.close()
                logger.info("Middleware de salida cerrado")
            
            if self.health_server:
                self.health_server.stop()
                self.health_server.join(timeout=5.0)
                logger.info("HealthChecker detenido")
            
        except Exception as e:
            logger.error(f"Error durante cleanup: {e}")
        
        logger.info("Cleanup completado")


if __name__ == "__main__":
    try:
        duplicate_filter = FilterDuplicateNode()
        duplicate_filter.start()
        logger.info("FilterDuplicateNode terminado exitosamente")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error fatal: {e}")
        sys.exit(1)