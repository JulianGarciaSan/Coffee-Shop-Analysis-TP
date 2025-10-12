import logging
from abc import ABC, abstractmethod
from dtos.dto import TransactionBatchDTO, BatchType
from rabbitmq.middleware import MessageMiddlewareExchange


logger = logging.getLogger(__name__)


class GroupByStrategy(ABC):
    def __init__(self):
        self.dto_helper = TransactionBatchDTO("", BatchType.RAW_CSV)
    
    @abstractmethod
    def process_csv_line(self, csv_line: str):
        pass
    
    @abstractmethod
    def generate_results_csv(self) -> str:
        pass
    
    def get_output_routing_key(self) -> str:
        raise NotImplementedError("Esta estrategia no usa routing keys estáticos")
    
    def get_eof_routing_key(self) -> str:
        raise NotImplementedError("Esta estrategia no usa routing keys estáticos")
    
    @abstractmethod
    def setup_output_middleware(self, rabbitmq_host: str, output_exchange: str):
        pass
    
    @abstractmethod
    def handle_eof_message(self, dto: TransactionBatchDTO, middlewares: dict) -> bool:
        pass
    
    def cleanup_middlewares(self, middlewares: dict):
        for name, middleware in middlewares.items():
            if middleware and hasattr(middleware, 'close'):
                try:
                    middleware.close()
                    logger.info(f"{name} middleware cerrado")
                except Exception as e:
                    logger.warning(f"Error cerrando {name}: {e}")
        