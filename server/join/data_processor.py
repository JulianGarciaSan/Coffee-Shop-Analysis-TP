from abc import ABC, abstractmethod
from typing import Dict
import logging

logger = logging.getLogger(__name__)

class DataProcessor(ABC):    
    def __init__(self, dto_helper):
        self.dto_helper = dto_helper
        self.data: Dict = {}
    
    @abstractmethod
    def process_line(self, line: str) -> bool:
        pass
    
    def process_batch(self, csv_data: str) -> int:
        processed_count = 0
        
        for line in csv_data.split('\n'):
            line = line.strip()
            if not line or self._should_skip_line(line):
                continue
            
            try:
                if self.process_line(line):
                    processed_count += 1
            except Exception as e:
                logger.warning(f"Error procesando línea: {line}, error: {e}")
                continue
        
        return processed_count
    
    def _should_skip_line(self, line: str) -> bool:
        return False
    
    def get_data(self) -> Dict:
        return self.data
    
    def clear(self):
        self.data.clear()