import logging
import os
from collections import defaultdict
from typing import Dict
from .base_strategy import GroupByStrategy
from dtos.dto import TransactionItemBatchDTO, BatchType

logger = logging.getLogger(__name__)


class ItemAggregation:
    def __init__(self, item_id: str):
        self.item_id = item_id
        self.sellings_qty = 0
        self.profit_sum = 0.0
    
    def add_transaction(self, quantity: int, subtotal: float): 
        self.sellings_qty += quantity  
        self.profit_sum += subtotal
    
    def to_csv_line(self, year_month: str) -> str:
        return f"{year_month},{self.item_id},{self.sellings_qty},{self.profit_sum:.2f}"


class BestSellingGroupByStrategy(GroupByStrategy):
    def __init__(self, input_queue_name: str, year: str = '2024'):
        super().__init__()  
        self.input_queue_name = input_queue_name
        self.year = year
        self.month_item_aggregations: Dict[str, Dict[str, ItemAggregation]] = defaultdict(
            lambda: defaultdict(lambda: None)
        )
        # Override dto_helper para usar TransactionItemBatchDTO
        self.dto_helper = TransactionItemBatchDTO("", BatchType.RAW_CSV)

        logger.info(f"BestSellingGroupByStrategy inicializada para año {year}")
    
    def process_csv_line(self, csv_line: str, client_id: str = 'default_client'):
        try:
            item_id = self.dto_helper.get_column_value(csv_line, 'item_id')
            created_at = self.dto_helper.get_column_value(csv_line, 'created_at')
            quantity_str = self.dto_helper.get_column_value(csv_line, 'quantity') 
            subtotal_str = self.dto_helper.get_column_value(csv_line, 'subtotal')
            
            if not all([item_id, created_at, quantity_str, subtotal_str]):
                return
            
            year_month = created_at[:7]
            quantity = int(quantity_str) 
            subtotal = float(subtotal_str)
            
            if self.month_item_aggregations[year_month][item_id] is None:
                self.month_item_aggregations[year_month][item_id] = ItemAggregation(item_id)
            
            self.month_item_aggregations[year_month][item_id].add_transaction(quantity, subtotal)  
            
        except (ValueError, IndexError) as e:
            logger.warning(f"Error procesando línea: {e}")