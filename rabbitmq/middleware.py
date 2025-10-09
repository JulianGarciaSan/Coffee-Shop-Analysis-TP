from abc import ABC, abstractmethod
import pika
import logging

logger = logging.getLogger(__name__)

class MessageMiddlewareMessageError(Exception):
    pass

class MessageMiddlewareDisconnectedError(Exception):
    pass

class MessageMiddlewareCloseError(Exception):
    pass

class MessageMiddlewareDeleteError(Exception):
    pass

class MessageMiddleware(ABC):

	#Comienza a escuchar a la cola/exchange e invoca a on_message_callback tras
	#cada mensaje de datos o de control.
	#Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	#Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
	@abstractmethod
	def start_consuming(self, on_message_callback):
		pass
	
	#Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
	#no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
	#Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	@abstractmethod
	def stop_consuming(self):
		pass
	
	#Envía un mensaje a la cola o al tópico con el que se inicializó el exchange.
	#Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	#Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
	@abstractmethod
	def send(self, message):
		pass

	#Se desconecta de la cola o exchange al que estaba conectado.
	#Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
	@abstractmethod
	def close(self):
		pass

	# Se fuerza la eliminación remota de la cola o exchange.
	# Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareDeleteError.
	@abstractmethod
	def delete(self):
		pass


class MessageMiddlewareExchange(MessageMiddleware):
    def __init__(self, host: str, exchange_name: str, route_keys: list):
        self.host = host
        self.exchange_name = exchange_name
        self.route_keys = route_keys if isinstance(route_keys, list) else [route_keys]
        self.connection = None
        self.channel = None
        self.consumer_queue = None  
        self.shutdown = None    
        
        self._connect()

    def _connect(self):
        try:
            credentials = pika.PlainCredentials('admin', 'admin')
            parameters = pika.ConnectionParameters(host=self.host, credentials=credentials)
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='topic', durable=True)
            self.channel.confirm_delivery()
            logger.info(f"Conectado a RabbitMQ. Exchange declarado: {self.exchange_name}")
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Error conectando a RabbitMQ: {e}")
            raise MessageMiddlewareDisconnectedError("No se pudo conectar a RabbitMQ")

    def send(self, message: str, routing_key: str = None, headers: dict = None):
        try:
            if not self.channel:
                raise MessageMiddlewareDisconnectedError("No hay conexión activa con RabbitMQ")
            
            key = routing_key if routing_key else self.route_keys[0]
            
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=key,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=1,
                    headers=headers or {}
                )
            )
        except MessageMiddlewareDisconnectedError:
            raise 
        except Exception as e:
            logger.error(f"Error enviando mensaje: {e}")
            raise MessageMiddlewareMessageError(f"Error enviando mensaje: {e}")

    def start_consuming(self, on_message_callback):
        try:
            if not self.channel:
                raise MessageMiddlewareDisconnectedError("No hay conexión activa con RabbitMQ")
            
            result = self.channel.queue_declare(queue='', exclusive=True)
            self.consumer_queue = result.method.queue
            
            for route_key in self.route_keys:
                self.channel.queue_bind(
                    exchange=self.exchange_name,
                    queue=self.consumer_queue,
                    routing_key=route_key
                )
            
            self.channel.basic_qos(prefetch_count=1)
            
            logger.info(f"Esperando mensajes en exchange {self.exchange_name} con routing keys {self.route_keys}...")
            
            for method_frame, properties, body in self.channel.consume(
                self.consumer_queue,
                auto_ack=False,
                inactivity_timeout=5  
            ):
                if self.shutdown and self.shutdown.is_shutting_down():
                    logger.info("Shutdown detectado en exchange middleware, dejando de consumir")
                    break
                
                if method_frame is None:
                    continue
                
                try:
                    on_message_callback(self.channel, method_frame, properties, body)
                    self.channel.basic_ack(method_frame.delivery_tag)
                except Exception as e:
                    logger.error(f"Error en callback: {e}")
                    self.channel.basic_nack(method_frame.delivery_tag, requeue=True)
                    
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Conexión con RabbitMQ perdida: {e}")
            raise MessageMiddlewareDisconnectedError("Conexión con RabbitMQ perdida")
        except MessageMiddlewareDisconnectedError:
            raise
        except Exception as e:
            logger.error(f"Error durante el consumo de mensajes: {e}")
            raise MessageMiddlewareMessageError(f"Error durante el consumo de mensajes: {e}")
        finally:
            logger.info("Finalizando consumo en exchange...")
            self.stop_consuming()    
    
    def _wrap_callback(self, user_callback):
        def wrapper(ch, method, properties, body):
            try:
                user_callback(ch, method, properties, body)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                logger.error(f"Error en callback: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return wrapper

    def stop_consuming(self):
        try:
            if self.channel:
                self.channel.stop_consuming()
                logger.info("Consumo de mensajes detenido")
        except Exception as e:
            logger.error(f"Error deteniendo el consumo: {e}")
            raise MessageMiddlewareMessageError(f"Error deteniendo el consumo: {e}")

    def delete(self):
        try:
            if self.channel:
                self.channel.exchange_delete(exchange=self.exchange_name)
                logger.info(f"Exchange {self.exchange_name} eliminado")
        except Exception as e:
            logger.error(f"Error eliminando el exchange: {e}")
            raise MessageMiddlewareDeleteError(f"Error eliminando el exchange: {e}")

    def close(self):
        try:
            if self.connection:
                self.connection.close()
                logger.info("Conexión con RabbitMQ cerrada")
        except Exception as e:
            logger.error(f"Error cerrando conexión: {e}")
            raise MessageMiddlewareCloseError(f"Error cerrando conexión: {e}")

class MessageMiddlewareQueue(MessageMiddleware):
    def __init__(self, host: str, queue_name: str, exchange_name: str = None, 
                 routing_keys: list = None):
        self.host = host
        self.queue_name = queue_name
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys or []
        self.connection = None
        self.channel = None
        self.shutdown = None

        self._connect()

    def _connect(self):
        try:
            credentials = pika.PlainCredentials('admin', 'admin')
            parameters = pika.ConnectionParameters(host=self.host, credentials=credentials)
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            
            self.channel.queue_declare(queue=self.queue_name, durable=True)
            
            if self.exchange_name and self.routing_keys:
                self.channel.exchange_declare(
                    exchange=self.exchange_name, 
                    exchange_type='topic', 
                    durable=True
                )
                
                for routing_key in self.routing_keys:
                    self.channel.queue_bind(
                        queue=self.queue_name,
                        exchange=self.exchange_name,
                        routing_key=routing_key
                    )
                    logger.info(f"Queue '{self.queue_name}' bindeada a '{self.exchange_name}' con routing_key '{routing_key}'")
            
            logger.info(f"Conectado a RabbitMQ. Cola declarada: {self.queue_name}")
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Error conectando a RabbitMQ: {e}")
            raise MessageMiddlewareDisconnectedError("No se pudo conectar a RabbitMQ")

    def send(self, message: str,headers: dict = None):
        try:
            if not self.channel:
                raise MessageMiddlewareDisconnectedError("No hay conexión activa con RabbitMQ")
            self.channel.basic_publish(
                exchange='',  
                routing_key=self.queue_name,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    headers=headers or {}
                )
            )
        except MessageMiddlewareDisconnectedError:
            raise 
        except Exception as e:
            logger.error(f"Error enviando mensaje: {e}")
            raise MessageMiddlewareMessageError(f"Error enviando mensaje: {e}")

    def start_consuming(self, on_message_callback):
        try:
            if not self.channel:
                raise MessageMiddlewareDisconnectedError("No hay conexión activa con RabbitMQ")
            
            # Optimizado para throughput
            self.channel.basic_qos(prefetch_count=10)
            
            for method_frame, properties, body in self.channel.consume(
                self.queue_name,
                auto_ack=False,
                inactivity_timeout=5
            ):
            
                if self.shutdown and self.shutdown.is_shutting_down():
                    logger.info("Shutdown detectado en middleware, dejando de consumir")
                    break
                
                if method_frame is None:
                    continue
                
                try:
                    on_message_callback(self.channel, method_frame, properties, body)
                    self.channel.basic_ack(method_frame.delivery_tag)
                except Exception as e:
                    logger.error(f"Error en callback: {e}")
                    self.channel.basic_nack(method_frame.delivery_tag, requeue=True)
                    
            self.channel.start_consuming()
        except MessageMiddlewareDisconnectedError:
            raise
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Conexión con RabbitMQ perdida: {e}")
            raise MessageMiddlewareDisconnectedError("Conexión con RabbitMQ perdida")
        except Exception as e:
            logger.error(f"Error durante el consumo de mensajes: {e}")
            raise MessageMiddlewareMessageError(f"Error durante el consumo de mensajes: {e}")
        finally:
            logger.info("Finalizando consumo...")
            self.stop_consuming()
            
    def _wrap_callback(self, user_callback):
        def wrapper(ch, method, properties, body):
            try:
                user_callback(ch, method, properties, body)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return wrapper

    def _callback_wrapper(self, ch, method, properties, body, on_message_callback):
        try:
            on_message_callback(ch, method, properties, body)
            ch.basic_ack(delivery_tag=method.delivery_tag)  
        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)  

    def stop_consuming(self):
        try:
            if self.channel:
                self.channel.stop_consuming()
                logger.info("Consumo de mensajes detenido")
        except Exception as e:
            logger.error(f"Error deteniendo el consumo: {e}")
            raise MessageMiddlewareMessageError(f"Error deteniendo el consumo: {e}")

    def delete(self):
        try:
            if self.channel:
                self.channel.queue_delete(queue=self.queue_name)
                logger.info(f"Cola {self.queue_name} eliminada")
        except Exception as e:
            logger.error(f"Error eliminando la cola: {e}")
            raise MessageMiddlewareDeleteError(f"Error eliminando la cola: {e}")

    def close(self):
        try:
            if self.connection:
                self.connection.close()
                logger.info("Conexión con RabbitMQ cerrada")
        except Exception as e:
            logger.error(f"Error cerrando conexión: {e}")
            raise MessageMiddlewareCloseError(f"Error cerrando conexión: {e}")
