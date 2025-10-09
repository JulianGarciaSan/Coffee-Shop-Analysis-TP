import logging
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class NodeConfigurator(ABC):
    def __init__(self, rabbitmq_host: str):
        self.rabbitmq_host = rabbitmq_host
    
    @abstractmethod
    def create_output_middlewares(self, output_q1: Optional[str], output_q3: Optional[str], 
                                  output_q4: Optional[str] = None, output_q2: Optional[str] = None) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    def process_filtered_data(self, filtered_csv: str) -> str:
        pass
    
    @abstractmethod
    def send_data(self, data: str, middlewares: Dict[str, Any], batch_type: str = "transactions"):
        pass
    
    @abstractmethod
    def send_eof(self, middlewares: Dict[str, Any], batch_type: str = "transactions"):
        pass
    
    @abstractmethod
    def handle_eof(self, counter: int, total_filters: int, eof_type: str, 
                   middlewares: Dict[str, Any], input_middleware: Any) -> bool:
        pass
    
    @abstractmethod
    def create_input_middleware(self, input_exchange: str, input_queue: str):
        pass
    
    @abstractmethod
    def process_message(self, body: bytes, routing_key: str = None) -> tuple:
        pass

