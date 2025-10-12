import logging
from typing import Callable, Dict

logger = logging.getLogger(__name__)

class MessageRouter:    
    def __init__(self):
        self.handlers: Dict[str, Callable] = {}
    
    def register(self, routing_key: str, handler: Callable):
        self.handlers[routing_key] = handler
    
    def route(self, routing_key: str, message: bytes) -> bool:
        handler = self.handlers.get(routing_key)
        if handler:
            return handler(message)
        else:
            logger.warning(f"No handler registrado para routing key: {routing_key}")
            return False
