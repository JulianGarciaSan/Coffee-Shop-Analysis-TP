import logging
import os
import sys
from rabbitmq.middleware import MessageMiddlewareExchange, MessageMiddlewareQueueManual
from common.graceful_shutdown import GracefulShutdown
from healthchecker.healthchecker import HealthChecker
from logger_monitor.logger_monitor import LoggerMonitor
from logger_monitor.logger_recovery import RecoveryManager
from typing import Set, Tuple, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FilterDuplicateNode:
    def __init__(self):
        self.shutdown = GracefulShutdown()
        self.shutdown.register_callback(self._on_shutdown_signal)
        
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.input_queue = os.getenv('INPUT_QUEUE', 'to_gateway_queue')
        self.output_exchange = os.getenv('OUTPUT_EXCHANGE', 'reports_exchange')
        
        logger.info(f"FilterDuplicateNode inicializado:")
        logger.info(f"  Input queue: {self.input_queue}")
        logger.info(f"  Output exchange: {self.output_exchange}")
        
        # UN SOLO LOG para todo (mensajes y EOFs)
        self.dedup_logger = LoggerMonitor('/app/dedup_log.txt')
        
        # Recovery manager para cargar estado inicial
        self.recovery = RecoveryManager(
            main_logger=self.dedup_logger,
            client_logger=self.dedup_logger,
            eof_logger=self.dedup_logger  # Mismo log para todo
        )
        
        # Cargar estado desde logs
        self.processed_messages, self.eof_clients = self.recovery.load_dedup_state()
        
        logger.info(f"Estado recuperado: {len(self.processed_messages)} mensajes procesados, "
                   f"{len(self.eof_clients)} clientes finalizados")
        
        # Middleware de entrada
        self.input_middleware = MessageMiddlewareQueueManual(
            host=self.rabbitmq_host,
            queue_name=self.input_queue,
            exchange_name='dedup_exchange',
            routing_keys=['q1.data']
        )
        
        if hasattr(self.input_middleware, 'shutdown'):
            self.input_middleware.shutdown = self.shutdown
        
        # Middleware de salida
        self.output_middleware = MessageMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name='reports_exchange',
            route_keys=['q1.data']
        )
        
        if hasattr(self.output_middleware, 'shutdown'):
            self.output_middleware.shutdown = self.shutdown
    
    def _on_shutdown_signal(self):
        """Callback para manejar señal de shutdown"""
        logger.info("FilterDuplicateNode: Señal de shutdown recibida")
        try:
            if self.input_middleware:
                self.input_middleware.stop_consuming()
        except Exception as e:
            logger.error(f"Error deteniendo consumo: {e}")
    
    def _extract_headers(self, properties) -> Tuple[Optional[int], Optional[int]]:
        """Extrae client_id y message_id de los headers"""
        client_id = None
        message_id = None
        
        if properties and properties.headers:
            client_id = properties.headers.get('client_id')
            message_id = properties.headers.get('message_id')
        
        return client_id, message_id
    
    def _is_eof_message(self, body: bytes) -> bool:
        """Verifica si el mensaje es un EOF"""
        try:
            decoded = body.decode('utf-8').strip()
            return decoded.startswith("EOF:")
        except:
            return False
    
    def is_duplicate(self, client_id: int, message_id: int) -> bool:
        """Verifica si un mensaje ya fue procesado"""
        return (client_id, message_id) in self.processed_messages
    
    def is_client_finished(self, client_id: int) -> bool:
        """Verifica si un cliente ya finalizó (basado en haber procesado su EOF)"""
        return client_id in self.eof_clients
    
    def mark_processed(self, client_id: int, message_id: int, is_eof: bool = False):
        """
        Marca un mensaje como procesado usando LoggerMonitor.
        Formato simple: CLIENT_ID:MSG_ID;
        """
        if (client_id, message_id) not in self.processed_messages:
            self.processed_messages.add((client_id, message_id))
            self.dedup_logger.write(f"{client_id}:{message_id}")
            
            # Si es EOF, también marcamos el cliente como finalizado
            if is_eof:
                self.eof_clients.add(client_id)
    
    def on_message_callback(self, ch, method, properties, body):
        """Callback para mensajes entrantes"""
        try:
            if self.shutdown.is_shutting_down():
                logger.warning("Shutdown en progreso, rechazando mensaje")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                ch.stop_consuming()
                return
            
            # Extraer headers
            client_id, message_id = self._extract_headers(properties)
            
            if client_id is None or message_id is None:
                logger.warning(f"Mensaje sin client_id o message_id en headers, rechazando")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return
            
            logger.debug(f"Mensaje recibido: client={client_id}, msg={message_id}")
            
            # Detectar si es EOF
            is_eof = self._is_eof_message(body)
            
            # ============ VERIFICACIONES COMUNES ============
            
            # Verificar si el cliente ya finalizó
            if not is_eof and self.is_client_finished(client_id):
                logger.info(f"Mensaje de cliente finalizado {client_id}, ignorando")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            # Verificar duplicación (tanto para EOF como para mensajes normales)
            if self.is_duplicate(client_id, message_id):
                logger.info(f"Mensaje duplicado detectado: client={client_id}, msg={message_id}, is_eof={is_eof}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            # ============ PROCESAMIENTO ============
            
            # Mensaje nuevo (EOF o normal) - enviar primero
            headers = {'client_id': client_id, 'message_id': message_id}
            self.output_middleware.send(body, routing_key='q1.data', headers=headers)
            
            if is_eof:
                logger.info(f"EOF enviado downstream: client={client_id}, msg={message_id}")
            else:
                logger.debug(f"Mensaje enviado downstream: client={client_id}, msg={message_id}")
            
            # Marcar como procesado (indicando si es EOF)
            self.mark_processed(client_id, message_id, is_eof=is_eof)
            
            # ACK
            ch.basic_ack(delivery_tag=method.delivery_tag)
        
        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}", exc_info=True)
            # En caso de error, rechazar con requeue para reintentar
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    
    def start(self):
        """Inicia el consumo de mensajes"""
        try:
            logger.info("Iniciando consumo de mensajes...")
            logger.info(f"Estado inicial: {len(self.processed_messages)} mensajes procesados, "
                       f"{len(self.eof_clients)} clientes finalizados")
            
            self.input_middleware.start_consuming(self.on_message_callback)
        
        except KeyboardInterrupt:
            logger.info("Detenido manualmente")
        
        except Exception as e:
            logger.error(f"Error durante consumo: {e}", exc_info=True)
            raise
        
        finally:
            self._cleanup()
    
    def _cleanup(self):
        """Limpieza de recursos"""
        logger.info("Iniciando cleanup...")
        
        try:
            if self.input_middleware:
                self.input_middleware.close()
                logger.info("Input middleware cerrado")
            
            if self.output_middleware:
                self.output_middleware.close()
                logger.info("Output middleware cerrado")
            
            # Cerrar logger
            if self.dedup_logger:
                self.dedup_logger.close()
        
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
        logger.error(f"Error fatal: {e}", exc_info=True)
        sys.exit(1)