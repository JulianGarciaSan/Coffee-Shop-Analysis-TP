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

class BestSellingGroupByStrategy(GroupByStrategy):
    def __init__(self, input_queue_name: str, year: str = '2024'):
        super().__init__()  
        self.input_queue_name = input_queue_name
        self.year = year
        self.month_item_aggregations_by_client: Dict[str, Dict[str, Dict[str, ItemAggregation]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(lambda: None))
        )
        self.dto_helper = TransactionItemBatchDTO("", BatchType.RAW_CSV)
        
        self.lines_processed_by_client = defaultdict(int)
        self.lines_per_item_by_client = defaultdict(lambda: defaultdict(int))

        logger.info(f"BestSellingGroupByStrategy inicializada para año {year}")
    
    def process_csv_line(self, csv_line: str, client_id: str = 'default_client'):
        try:
            item_id = self.dto_helper.get_column_value(csv_line, 'item_id')
            created_at = self.dto_helper.get_column_value(csv_line, 'created_at')
            quantity_str = self.dto_helper.get_column_value(csv_line, 'quantity') 
            subtotal_str = self.dto_helper.get_column_value(csv_line, 'subtotal')
            
            if not all([item_id, created_at, quantity_str, subtotal_str]):
                return
            
            self.lines_processed_by_client[client_id] += 1
            self.lines_per_item_by_client[client_id][item_id] += 1
            
            year_month = created_at[:7]
            quantity = int(quantity_str) 
            subtotal = float(subtotal_str)
            
            if self.month_item_aggregations_by_client[client_id][year_month][item_id] is None:
                self.month_item_aggregations_by_client[client_id][year_month][item_id] = ItemAggregation(item_id)
            
            self.month_item_aggregations_by_client[client_id][year_month][item_id].add_transaction(quantity, subtotal)  

        except (ValueError, IndexError) as e:
            logger.warning(f"Error procesando línea para client_id={client_id}: {e}")
            
            
    def _serialize_client_data(self, client_id: str) -> Dict:
        """
        Serializa el estado de BestSelling a un diccionario.
        
        Estructura:
        {
            "2024-01": {
                "item_123": {
                    "sellings_qty": 150,
                    "profit_sum": 4500.50
                },
                "item_456": {
                    "sellings_qty": 89,
                    "profit_sum": 2340.75
                }
            },
            "2024-02": {
                ...
            }
        }
        """
        client_aggregations = self.month_item_aggregations_by_client.get(client_id, {})
        serialized = {}
        
        for year_month, items_dict in client_aggregations.items():
            serialized[year_month] = {}
            
            for item_id, aggregation in items_dict.items():
                if aggregation is not None:  # Solo serializar agregaciones que existen
                    serialized[year_month][item_id] = {
                        "sellings_qty": aggregation.sellings_qty,
                        "profit_sum": aggregation.profit_sum
                    }
        
        # serialized["_stats"] = {
        #     "lines_processed": self.lines_processed_by_client.get(client_id, 0),
        #     "lines_per_item": dict(self.lines_per_item_by_client.get(client_id, {}))
        # }
        
        return serialized

    def _deserialize_client_data(self, client_id: str, data: Dict):
        """
        Reconstruye el estado de BestSelling desde un diccionario.
        
        Args:
            client_id: ID del cliente
            data: Diccionario con estructura {year_month: {item_id: {sellings_qty, profit_sum}}}
        """
        # if "_stats" in data:
        #     stats = data["_stats"]
        #     self.lines_processed_by_client[client_id] = stats.get("lines_processed", 0)
        #     if "lines_per_item" in stats:
        #         self.lines_per_item_by_client[client_id] = defaultdict(int, stats["lines_per_item"])
            
        #     data = {k: v for k, v in data.items() if k != "_stats"}
        
        for year_month, items_dict in data.items():
            for item_id, aggregation_data in items_dict.items():
                if self.month_item_aggregations_by_client[client_id][year_month][item_id] is None:
                    self.month_item_aggregations_by_client[client_id][year_month][item_id] = ItemAggregation(item_id)
                
                aggregation = self.month_item_aggregations_by_client[client_id][year_month][item_id]
                aggregation.sellings_qty = aggregation_data.get('sellings_qty', 0)
                aggregation.profit_sum = aggregation_data.get('profit_sum', 0.0)
        
        logger.info(f"BestSelling deserializado para cliente {client_id}:")
        logger.info(f"   Meses: {len(self.month_item_aggregations_by_client[client_id])}")
        logger.info(f"   Items únicos: {sum(len(items) for items in self.month_item_aggregations_by_client[client_id].values())}")
        logger.info(f"   Líneas procesadas: {self.lines_processed_by_client[client_id]}")