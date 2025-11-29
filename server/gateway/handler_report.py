# report_handler.py
import threading
from typing import Optional, Dict, Tuple
from collections import OrderedDict
from rabbitmq.middleware import MessageMiddlewareQueue
from logger import get_logger

logger = get_logger(__name__)


class LRUCache:
    """Cache LRU simple para tracking de duplicados."""
    
    def __init__(self, max_size: int = 10000):
        self.cache: OrderedDict[Tuple[int, int, str], bool] = OrderedDict()
        self.max_size = max_size
    
    def contains(self, client_id: int, message_id: int, query: str) -> bool:
        """Verifica si un mensaje ya fue procesado."""
        key = (client_id, message_id, query)
        
        if key in self.cache:
            self.cache.move_to_end(key)
            return True
        
        return False
    
    def add(self, client_id: int, message_id: int, query: str):
        """Agrega un mensaje al cache."""
        key = (client_id, message_id, query)
        
        if key in self.cache:
            self.cache.move_to_end(key)
            return
        
        self.cache[key] = True
        
        if len(self.cache) > self.max_size:
            oldest = next(iter(self.cache))
            del self.cache[oldest]
    
    def size(self) -> int:
        """Retorna el tamaño actual del cache."""
        return len(self.cache)
    
    def clear(self):
        """Limpia el cache."""
        self.cache.clear()


class ReportHandler(threading.Thread):

    REPORT_QUEUE_NAME = "gateway_reports"

    def __init__(self, rabbitmq_host: str, reports_exchange: str, gateway, 
                 shutdown_handler=None, cache_size: int = 10000):
        super().__init__(daemon=True)
        self._gateway = gateway
        self._shutdown = shutdown_handler
        self._host = rabbitmq_host
        self._exchange = reports_exchange
        
        self._dedup_cache = LRUCache(max_size=cache_size)
        self._cache_hits = 0 
        self._total_messages = 0
        
        self._middleware = MessageMiddlewareQueue(
            host=self._host,
            queue_name=self.REPORT_QUEUE_NAME,
            exchange_name='reports_exchange',
            routing_keys=[
                'q1.data', 
                'q2.data', 
                'q3.data', 
                'q4.data',
                'q2_most_profit.data',
                'q2_best_selling.data'
            ]
        )

        if self._shutdown and hasattr(self._middleware, "shutdown"):
            self._middleware.shutdown = self._shutdown

        self._stopped = threading.Event()
        
        logger.info(f"ReportHandler inicializado con cache size={cache_size}")

    def run(self):
        logger.info("ReportHandler iniciado")
        try:
            self._middleware.start_consuming(self._on_message_callback)
        except Exception as exc: 
            logger.error(f"ReportHandler detenido por error: {exc}")
        finally:
            self._log_stats()
            self._stopped.set()
            self.cleanup()
            logger.info("ReportHandler detenido")

    def stop(self):
        logger.info("Deteniendo ReportHandler...")
        try:
            self._middleware.stop_consuming()
        except Exception as exc: 
            logger.error(f"Error deteniendo ReportHandler: {exc}")

    def cleanup(self):
        try:
            self._middleware.close()
            self._dedup_cache.clear()
        except Exception as exc: 
            logger.error(f"Error cerrando middleware de reportes: {exc}")

    def _on_message_callback(self, channel, method, properties, body):
        if self._shutdown and self._shutdown.is_shutting_down():
            logger.info("Shutdown detectado en ReportHandler")
            channel.stop_consuming()
            return

        client_id = self._extract_client_id(properties)
        message_id = self._extract_message_id(properties)
        routing_key = getattr(method, "routing_key", "")
        
        if client_id is None:
            logger.warning(
                "Mensaje de reporte sin client_id, descartando. routing_key=%s",
                routing_key,
            )
            return
        
        query_name = self._extract_query_name(routing_key)
        
        self._total_messages += 1
        
        if message_id is not None and self._is_duplicate(client_id, message_id, query_name):
            self._cache_hits += 1
            logger.info(
                f"Reporte duplicado filtrado: client={client_id}, msg={message_id}, "
                f"query={query_name} (hits={self._cache_hits}/{self._total_messages})"
            )
            return
        
        if message_id is not None:
            self._mark_as_processed(client_id, message_id, query_name)
        
        self._gateway.dispatch_report_to_client(client_id, routing_key, body)
                
        if self._total_messages % 1000 == 0:
            self._log_stats()

    def _is_duplicate(self, client_id: int, message_id: int, query: str) -> bool:
        """Verifica si el mensaje ya fue procesado."""
        return self._dedup_cache.contains(client_id, message_id, query)
    
    def _mark_as_processed(self, client_id: int, message_id: int, query: str):
        """Marca un mensaje como procesado."""
        self._dedup_cache.add(client_id, message_id, query)
    
    def _extract_query_name(self, routing_key: str) -> str:
        """
        Extrae el nombre de la query desde el routing_key.
        
        Ejemplos:
        - 'q1.data' → 'q1'
        - 'q2_most_profit.data' → 'q2_most_profit'
        - 'q3.data' → 'q3'
        """
        if not routing_key:
            return "unknown"
        
        if routing_key.endswith('.data'):
            return routing_key[:-5]
        
        return routing_key
    
    def _log_stats(self):
        """Log de estadísticas de deduplicación."""
        cache_size = self._dedup_cache.size()
        hit_rate = (self._cache_hits / self._total_messages * 100) if self._total_messages > 0 else 0
        
        logger.info(
            f"ReportHandler stats: total={self._total_messages}, "
            f"duplicados={self._cache_hits} ({hit_rate:.2f}%), "
            f"cache_size={cache_size}"
        )

    @staticmethod
    def _extract_client_id(properties) -> Optional[int]:
        if not properties or not getattr(properties, "headers", None):
            return None
        client_id = properties.headers.get("client_id")
        if client_id is None:
            return None
        try:
            return int(client_id)
        except (TypeError, ValueError):
            logger.warning("client_id inválido en headers: %s", client_id)
            return None
    
    @staticmethod
    def _extract_message_id(properties) -> Optional[int]:
        """Extrae message_id de los headers."""
        if not properties or not getattr(properties, "headers", None):
            return None
        message_id = properties.headers.get("message_id")
        if message_id is None:
            return None
        try:
            if isinstance(message_id, str) and '_' in message_id:
                message_id = message_id.split('_')[-1]
            return int(message_id)
        except (TypeError, ValueError):
            logger.warning("message_id inválido en headers: %s", message_id)
            return None