from abc import ABC, abstractmethod
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)

class FilterStrategy(ABC):
    @abstractmethod
    def should_keep_line(self, csv_line: str) -> bool:
        pass

    def filter_csv_batch(self, csv_data: str) -> str:
        lines = csv_data.split('\n')
        filtered_lines = []
        
        for line in lines:
            if line.strip() and self.should_keep_line(line):
                filtered_lines.append(line)
        
        return '\n'.join(filtered_lines)