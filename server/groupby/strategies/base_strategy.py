import logging
from abc import ABC, abstractmethod
from dtos.dto import TransactionBatchDTO, BatchType


logger = logging.getLogger(__name__)


class GroupByStrategy(ABC):
    """
    Clase base para estrategias de agrupación.
    Solo contiene lógica de negocio (procesamiento de datos).
    El configurator maneja middlewares y EOF.
    """
    def __init__(self):
        self.dto_helper = TransactionBatchDTO("", BatchType.RAW_CSV)
    
    @abstractmethod
    def process_csv_line(self, csv_line: str):
        """Procesa una línea CSV y acumula datos"""
        pass
