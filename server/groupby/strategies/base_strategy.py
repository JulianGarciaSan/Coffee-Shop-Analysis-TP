import json
import logging
import os
import tempfile
from abc import ABC, abstractmethod
from typing import Dict, Any
from message_range_tracker.message_range_tracker import MessageRangeTracker

from dtos.dto import BatchType, TransactionBatchDTO

logger = logging.getLogger(__name__)


class GroupByStrategy(ABC):
    
    def __init__(self):
        self.client_logger = None
        # self.eof_logger = None
        self.dto_helper = TransactionBatchDTO("", BatchType.RAW_CSV)
        
        self.message_trackers: Dict[str, MessageRangeTracker] = {}
        
        # if checkpoint_dir:
        #     self.checkpoint_dir = checkpoint_dir
        # else:
        #     self.checkpoint_dir = '/app/server/logs/checkpoints'
        
        # os.makedirs(self.checkpoint_dir, exist_ok=True)
        # logger.info(f"Sistema de checkpoints inicializado en {self.checkpoint_dir}")
    
    def set_loggers(self, client_logger):
        self.client_logger = client_logger
    
    @abstractmethod
    def process_csv_line(self, csv_line: str, client_id: str = 'default_client'):
        """Procesa una línea CSV y actualiza el estado en memoria"""
        pass
    
    @abstractmethod
    def _serialize_client_data(self, client_id: str) -> Dict[str, Any]:
        """
        Serializa el estado en memoria de un cliente a un diccionario.
        """
        pass
    
    @abstractmethod
    def _deserialize_client_data(self, client_id: str, data: Dict[str, Any]):
        """
        Reconstruye el estado en memoria desde un diccionario.
        """
        pass
        
        
    # def get_checkpoint_info(self, client_id: str) -> Dict[str, Any]:
    #     """
    #     Obtiene información del checkpoint de un cliente sin cargarlo completamente.
    #     """
    #     checkpoint_path = os.path.join(self.checkpoint_dir, f"client_{client_id}.checkpoint")
        
    #     if not os.path.exists(checkpoint_path):
    #         return {'exists': False}
        
    #     try:
    #         with open(checkpoint_path, 'r') as f:
    #             checkpoint_data = json.load(f)
            
    #         return {
    #             'exists': True,
    #             'last_message_id': checkpoint_data.get('last_message_id'),
    #             'strategy_type': checkpoint_data.get('strategy_type'),
    #             'file_size': os.path.getsize(checkpoint_path)
    #         }
    #     except Exception as e:
    #         logger.warning(f"Error leyendo info de checkpoint {client_id}: {e}")
    #         return {'exists': True, 'error': str(e)}