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
        self.logger = None
        self.logger_client = None
        self.eof_logger = None

    def set_loggers(self, logger, logger_client, eof_logger):
        self.logger = logger
        self.logger_client = logger_client
        self.eof_logger = eof_logger

    @abstractmethod
    def process_csv_line(self, csv_line: str, client_id: str = 'default_client'):
        """Procesa una línea CSV y acumula datos"""
        pass