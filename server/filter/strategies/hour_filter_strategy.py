import logging
from datetime import datetime
from .base_strategy import FilterStrategy
from dtos.dto import TransactionBatchDTO, BatchType

logger = logging.getLogger(__name__)

class HourFilterStrategy(FilterStrategy):
    
    def __init__(self, filter_hours: str):
        self.filter_hours = filter_hours
        start_hour, end_hour = filter_hours.split('-')
        self.start_time = datetime.strptime(start_hour, "%H:%M").time()
        self.end_time = datetime.strptime(end_hour, "%H:%M").time()
        self.count = 0
        self.dto_helper = TransactionBatchDTO("", BatchType.RAW_CSV)
        
    def should_keep_line(self, csv_line: str) -> bool:
        try:
            created_at = self.dto_helper.get_column_value(csv_line, 'created_at')
            
            if not created_at or ' ' not in created_at:
                return False
            
            time_part = created_at.split(' ')[1] 
            hour_minute = time_part[:5] 
            
            if self.start_time.strftime("%H:%M") <= hour_minute <= self.end_time.strftime("%H:%M"):
                self.count += 1
                if self.count % 1000 == 0:
                    logger.info(f"HourFilter: {self.count} transacciones pasaron el filtro")
                return True
            return False
            
        except (IndexError, ValueError):
            return False