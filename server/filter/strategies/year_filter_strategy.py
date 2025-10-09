import logging
from datetime import datetime
from .base_strategy import FilterStrategy
from dtos.dto import TransactionBatchDTO, TransactionItemBatchDTO, BatchType

logger = logging.getLogger(__name__)

class YearFilterStrategy(FilterStrategy):
    def __init__(self, filter_years: list):
        self.filter_years = filter_years
        self.count = 0
        self.dto_helper = None
        
    def set_dto_helper(self, dto_helper):
        self.dto_helper = dto_helper

    def should_keep_line(self, csv_line: str) -> bool:
        try:
            if not self.dto_helper:
                logger.warning("DTO helper no configurado, usando fallback")
                return False
                
            created_at = self.dto_helper.get_column_value(csv_line, 'created_at')
            
            if not created_at or len(created_at) < 4:
                return False
            
            year_str = created_at[:4]
            
            if year_str in self.filter_years:
                self.count += 1
                if self.count % 1000 == 0:
                    logger.info(f"YearFilter: {self.count} líneas pasaron el filtro")
                return True
            return False
            
        except (IndexError, ValueError) as e:
            logger.debug(f"Error filtrando línea: {e}")
            return False
    
    def get_filter_description(self) -> str:
        return f"Filtro por año: {', '.join(self.filter_years)}"