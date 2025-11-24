import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

from dtos.dto import BatchType, TransactionBatchDTO

logger = logging.getLogger(__name__)


class GroupByConfigurator(ABC):    
    def __init__(self, rabbitmq_host: str, output_exchange: str):
        self.rabbitmq_host = rabbitmq_host
        self.output_exchange = output_exchange
        self.dto_helper = TransactionBatchDTO("", BatchType.RAW_CSV)
        self.logger = None
        self.logger_client = None
        self.eof_logger = None

    
    @abstractmethod
    def create_input_middleware(self):
        pass
    
    @abstractmethod
    def create_output_middlewares(self) -> Dict[str, Any]:
        pass
    
    # @abstractmethod
    # def process_message(self, body: bytes, headers: dict = None) -> tuple:
    #     """
    #     Procesa un mensaje recibido.
        
    #     Args:
    #         body: El cuerpo del mensaje en bytes
    #         headers: Headers del mensaje (puede contener client_id)
            
    #     Returns:
    #         tuple: (should_stop, dto, is_eof)
    #     """
    #     pass
    
    @abstractmethod
    def get_strategy_config(self) -> dict:
        pass
    
    @abstractmethod
    def handle_eof(self, dto: TransactionBatchDTO, middlewares: dict, strategy, client_id: str, message_id: str) -> bool:
        pass
    
    def set_loggers(self, logger, logger_client, eof_logger):
        self.logger = logger
        self.logger_client = logger_client
        self.eof_logger = eof_logger
        
    def create_headers(self, client_id: Optional[int], message_id: Optional[int]) -> Dict[str, Any]:
        headers = {}
        if client_id is not None and message_id is not None:
            return {'client_id': client_id,
                    'message_id': message_id
                    }
        return {}
