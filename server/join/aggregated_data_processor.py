from typing import Callable, Dict, List
import logging

logger = logging.getLogger(__name__)

class AggregatedDataProcessor:    
    def __init__(self):
        self.data: List[Dict] = []
    
    def process_batch(self, csv_data: str, parser_func: Callable) -> int:
        processed_count = 0
        
        for line in csv_data.split('\n'):
            line = line.strip()
            if not line or self._should_skip_line(line):
                continue
            
            try:
                parsed = parser_func(line)
                if parsed:
                    self.data.append(parsed)
                    processed_count += 1
            except Exception as e:
                logger.warning(f"Error procesando línea: {line}, error: {e}")
                continue
        
        return processed_count
    
    def _should_skip_line(self, line: str) -> bool:
        return False
    
    def get_data(self) -> List[Dict]:
        return self.data
    
    def clear(self):
        if self.data is not None:
            self.data.clear()
        else:
            logger.warning("AggregatedDataProcessor.data era None durante clear()")
            self.data = []
