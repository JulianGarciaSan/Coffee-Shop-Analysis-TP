import csv
import logging
from typing import List
from dataclasses import dataclass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class BatchResult:
    items: List[str]  
    is_eof: bool

class TransactionCSVProcessor:
    
    def __init__(self, csv_filepath: str, batch_size: int = 100):
        self.csv_filepath = csv_filepath
        self.batch_size = batch_size
        self.is_eof = False
        self.file = None
        self.count = 0
        
        self._open_file()
    
    def _open_file(self):
        try:
            self.file = open(self.csv_filepath, 'r', encoding='utf-8')
            logger.info(f"Archivo CSV abierto: {self.csv_filepath}")
        except Exception as e:
            logger.error(f"Error abriendo archivo: {e}")
            raise
    
    def has_more_batches(self) -> bool:
        return not self.is_eof
    
    def read_next_batch(self) -> BatchResult:
        """Lee el siguiente batch de líneas del archivo CSV."""
        if self.is_eof:
            return BatchResult(items=[], is_eof=True)
        
        items = []
        
        try:
            for _ in range(self.batch_size):
                line = self.file.readline()
                if not line:
                    self.is_eof = True
                    break
                items.append(line.strip())
                    
        except Exception as e:
            logger.error(f"Error leyendo batch: {e}")
            return BatchResult(items=[], is_eof=True)
        
        
        self.count += 1
        # logger.info(f"Batch numero: {self.count} ")
        return BatchResult(items=items, is_eof=self.is_eof)
    
    def close(self):
        if self.file:
            self.file.close()
            logger.info("Archivo CSV cerrado")
