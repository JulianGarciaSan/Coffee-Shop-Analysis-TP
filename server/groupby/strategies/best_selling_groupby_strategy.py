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
        
        # self.lines_processed_by_client = defaultdict(int)
        # self.lines_per_item_by_client = defaultdict(lambda: defaultdict(int))

        logger.info(f"BestSellingGroupByStrategy inicializada para año {year}")
    
    def process_csv_line(self, csv_line: str, client_id: str = 'default_client'):
        try:
            item_id = self.dto_helper.get_column_value(csv_line, 'item_id')
            created_at = self.dto_helper.get_column_value(csv_line, 'created_at')
            quantity_str = self.dto_helper.get_column_value(csv_line, 'quantity') 
            subtotal_str = self.dto_helper.get_column_value(csv_line, 'subtotal')
            
            if not all([item_id, created_at, quantity_str, subtotal_str]):
                return
            
            # self.lines_processed_by_client[client_id] += 1
            # self.lines_per_item_by_client[client_id][item_id] += 1
            
            year_month = created_at[:7]
            quantity = int(quantity_str) 
            subtotal = float(subtotal_str)
            
            if self.month_item_aggregations_by_client[client_id][year_month][item_id] is None:
                self.month_item_aggregations_by_client[client_id][year_month][item_id] = ItemAggregation(item_id)
            
            self.month_item_aggregations_by_client[client_id][year_month][item_id].add_transaction(quantity, subtotal)  

        except (ValueError, IndexError) as e:
            logger.warning(f"Error procesando línea para client_id={client_id}: {e}")
            
    def serialize_operation(self, client_id: str, csv_line: str) -> str:
        """
        Serializa una operación BestSelling para el WAL.
        
        Formato: client_id,item_id,created_at,quantity,subtotal
        Ejemplo: 0,item_123,2024-01-15,5,50.00
        """
        try:
            item_id = self.dto_helper.get_column_value(csv_line, 'item_id')
            created_at = self.dto_helper.get_column_value(csv_line, 'created_at')
            quantity = self.dto_helper.get_column_value(csv_line, 'quantity')
            subtotal = self.dto_helper.get_column_value(csv_line, 'subtotal')
            
            if not all([item_id, created_at, quantity, subtotal]):
                return None
            
            return f"{client_id},{item_id},{created_at},{quantity},{subtotal}"
            
        except Exception as e:
            logger.error(f"Error serializando operación BestSelling: {e}")
            raise

    def deserialize_operation(self, operation_str: str) -> dict:
        """
        Deserializa una operación BestSelling desde el WAL.
        
        Input: "0,item_123,2024-01-15,5,50.00"
        Output: {
            'client_id': '0',
            'item_id': 'item_123',
            'created_at': '2024-01-15',
            'quantity': 5,
            'subtotal': 50.00
        }
        """
        try:
            parts = operation_str.split(',')
            
            if len(parts) != 5:
                raise ValueError(f"Formato inválido, esperaba 5 campos, encontró {len(parts)}: {operation_str}")
            
            client_id, item_id, created_at, quantity_str, subtotal_str = parts
            
            return {
                'client_id': client_id,
                'item_id': item_id,
                'created_at': created_at,
                'quantity': int(quantity_str),
                'subtotal': float(subtotal_str)
            }
            
        except Exception as e:
            logger.error(f"Error deserializando operación BestSelling: {e}")
            raise

    def apply_operation(self, operation: dict):
        """
        Aplica una operación BestSelling al estado en memoria.
        Similar a process_csv_line pero desde dict ya parseado.
        """
        try:
            client_id = operation['client_id']
            item_id = operation['item_id']
            created_at = operation['created_at']
            quantity = operation['quantity']
            subtotal = operation['subtotal']
            
            year_month = created_at[:7]  
            
            if self.month_item_aggregations_by_client[client_id][year_month][item_id] is None:
                self.month_item_aggregations_by_client[client_id][year_month][item_id] = ItemAggregation(item_id)
            
            self.month_item_aggregations_by_client[client_id][year_month][item_id].add_transaction(quantity, subtotal)
            
            # self.lines_processed_by_client[client_id] += 1
            # self.lines_per_item_by_client[client_id][item_id] += 1
            
        except Exception as e:
            logger.error(f"Error aplicando operación BestSelling: {e}")
            raise
    
    def _serialize_client_data(self, client_id: str) -> Dict:
        """
        Serializa el estado de BestSelling a un diccionario.
        
        Estructura:
        {
            "2024-01": {
                "item_123": {
                    "sellings_qty": 150,
                    "profit_sum": 4500.50
                }
            }
        }
        """
        client_aggregations = self.month_item_aggregations_by_client.get(client_id, {})
        serialized = {}
        
        for year_month, items_dict in client_aggregations.items():
            serialized[year_month] = {}
            
            for item_id, aggregation in items_dict.items():
                if aggregation is not None: 
                    serialized[year_month][item_id] = {
                        "sellings_qty": aggregation.sellings_qty,
                        "profit_sum": aggregation.profit_sum
                    }
        
        return serialized

    def _deserialize_client_data(self, client_id: str, data: Dict):
        """
        Reconstruye el estado de BestSelling desde un diccionario.
        
        data: Diccionario con estructura {year_month: {item_id: {sellings_qty, profit_sum}}}
        """
        self.month_item_aggregations_by_client[client_id] = defaultdict(lambda: defaultdict(lambda: None))
        
        for year_month, items_dict in data.items():
            for item_id, aggregation_data in items_dict.items():
                self.month_item_aggregations_by_client[client_id][year_month][item_id] = ItemAggregation(item_id)
                
                aggregation = self.month_item_aggregations_by_client[client_id][year_month][item_id]
                aggregation.sellings_qty = aggregation_data.get('sellings_qty', 0)
                aggregation.profit_sum = aggregation_data.get('profit_sum', 0.0)
