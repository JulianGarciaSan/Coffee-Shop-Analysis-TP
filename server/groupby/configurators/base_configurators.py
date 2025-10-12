import logging
from abc import ABC, abstractmethod
from typing import Dict, Any

from dtos.dto import BatchType, TransactionBatchDTO

logger = logging.getLogger(__name__)


class GroupByConfigurator(ABC):    
    def __init__(self, rabbitmq_host: str, output_exchange: str):
        self.rabbitmq_host = rabbitmq_host
        self.output_exchange = output_exchange
        self.dto_helper = TransactionBatchDTO("", BatchType.RAW_CSV)
    
    @abstractmethod
    def create_input_middleware(self):
        pass
    
    @abstractmethod
    def create_output_middlewares(self) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    def process_message(self, body: bytes) -> tuple:
        pass
    
    @abstractmethod
    def get_strategy_config(self) -> dict:
        pass
    
    @abstractmethod
    def handle_eof(self, dto: TransactionBatchDTO, middlewares: dict, strategy) -> bool:
        pass
