import logging
from .base_strategy import FilterStrategy
from dtos.dto import TransactionBatchDTO, BatchType

logger = logging.getLogger(__name__)

class AmountFilterStrategy(FilterStrategy):
    
    def __init__(self, min_amount: float):
        self.min_amount = min_amount
        self.count = 0
        self.dto_helper = TransactionBatchDTO("", BatchType.RAW_CSV)
        

    def should_keep_line(self, csv_line: str) -> bool:
        try:
            final_amount_str = self.dto_helper.get_column_value(csv_line, 'final_amount')
            
            if not final_amount_str:
                return False
            
            final_amount = float(final_amount_str)
            
            if final_amount >= self.min_amount:
                self.count += 1
                if self.count % 1000 == 0:
                    logger.info(f"AmountFilter: {self.count} transacciones pasaron el filtro")
                return True
            return False
            
        except (ValueError, TypeError):
            return False