import logging
import os
from collections import defaultdict
from typing import Any, Dict, Optional
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
    OP_START_TX = 'START_TRANSACTION'
    OP_COMMIT_TX = 'COMMIT_TRANSACTION'
    OP_SENT = 'SENT'
    OP_EOF = 'EOF'
    OP_DATA = 'DATA'
    OP_COUNTER = 'COUNTER'
    
    def __init__(self, input_queue_name: str, year: str = '2024',outgoing_counter_by_client: Dict[str, int] = None):
        super().__init__()  
        self.input_queue_name = input_queue_name
        self.year = year
        self.month_item_aggregations_by_client: Dict[str, Dict[str, Dict[str, ItemAggregation]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(lambda: None))
        )
        self.dto_helper = TransactionItemBatchDTO("", BatchType.RAW_CSV)
        self.outgoing_counter_by_client = outgoing_counter_by_client or {}

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
            
            if self.month_item_aggregations_by_client[client_id][year_month][item_id] is None:
                self.month_item_aggregations_by_client[client_id][year_month][item_id] = ItemAggregation(item_id)
            
            self.month_item_aggregations_by_client[client_id][year_month][item_id].add_transaction(quantity, subtotal)  

        except (ValueError, IndexError) as e:
            logger.warning(f"Error procesando línea para client_id={client_id}: {e}")
            
    def serialize_operation(self, client_id: str, csv_line: str) -> Optional[str]:
        """
        Serializa operaciones para el WAL.
        """
        try:
            if any(op in csv_line for op in [self.OP_START_TX, self.OP_COMMIT_TX, self.OP_SENT, self.OP_EOF, 'PENDING_ID', 'COMMIT_ID']):
                if csv_line.startswith(f"{client_id},"):
                    return csv_line
                return f"{client_id},{csv_line}"

            item_id = self.dto_helper.get_column_value(csv_line, 'item_id')
            created_at = self.dto_helper.get_column_value(csv_line, 'created_at')
            quantity = self.dto_helper.get_column_value(csv_line, 'quantity')
            subtotal = self.dto_helper.get_column_value(csv_line, 'subtotal')
            
            if not all([item_id, created_at, quantity, subtotal]):
                return None
            
            return f"{client_id},{self.OP_DATA},{item_id},{created_at},{quantity},{subtotal}"
            
        except Exception as e:
            logger.error(f"Error serializando operación BestSelling: {e}")
            return None

    def deserialize_operation(self, operation_str: str) -> Dict[str, Any]:
        """
        Deserializa operaciones del WAL a diccionarios tipados.
        """
        try:
            operation_str = operation_str.strip()
            if not operation_str:
                raise ValueError("Línea vacía")

            parts = operation_str.split(',', 2)
            if len(parts) < 2:
                raise ValueError(f"Formato inválido: {operation_str}")
            
            client_id = parts[0]
            op_part = parts[1].strip()
            content = parts[2] if len(parts) > 2 else ""

            if ':' in op_part:
                op_type, extra = op_part.split(':', 1)
                if not content: content = extra
            else:
                op_type = op_part

            if op_type == self.OP_START_TX:
                if ',' in content:
                    msg_id_str, query = content.split(',', 1)
                    return {'client_id': client_id, 'type': self.OP_START_TX, 'id': int(msg_id_str), 'query': query}
                return {'client_id': client_id, 'type': self.OP_START_TX, 'id': int(content)}

            if op_type == self.OP_COMMIT_TX:
                return {'client_id': client_id, 'type': self.OP_COMMIT_TX, 'id': int(content)}

            if op_type == self.OP_SENT:
                return {'client_id': client_id, 'type': self.OP_SENT, 'query': content}

            if op_type == self.OP_EOF:
                return {'client_id': client_id, 'type': self.OP_EOF, 'eof_type': content}
            
            if op_type == self.OP_COUNTER:
                return {'client_id': client_id, 'type': self.OP_COUNTER, 'value': int(content)}

            if op_type == self.OP_DATA:
                data_parts = content.split(',')
                if len(data_parts) < 4:
                    raise ValueError(f"Datos incompletos para DATA: {content}")
                
                return {
                    'client_id': client_id,
                    'type': self.OP_DATA,
                    'item_id': data_parts[0],
                    'created_at': data_parts[1],
                    'quantity': int(data_parts[2]),
                    'subtotal': float(data_parts[3])
                }

            if ',' in content and len(content.split(',')) == 3:
                 legacy_parts = content.split(',')
                 return {
                    'client_id': client_id,
                    'type': self.OP_DATA,
                    'item_id': op_part,
                    'created_at': legacy_parts[0],
                    'quantity': int(legacy_parts[1]),
                    'subtotal': float(legacy_parts[2])
                }

            raise ValueError(f"Opcode desconocido: {op_type}")

        except Exception as e:
            logger.error(f"Error deserializando '{operation_str}': {e}")
            raise

    def apply_operation(self, operation: Dict[str, Any]):
        """
        Aplica la operación al estado en memoria.
        """
        try:
            client_id = operation['client_id']
            op_type = operation.get('type', 'data')
            
            if op_type in [self.OP_START_TX, self.OP_COMMIT_TX, self.OP_SENT, self.OP_EOF, self.OP_COUNTER]:
                return

            if op_type == self.OP_DATA:
                item_id = operation['item_id']
                created_at = operation['created_at']
                quantity = operation['quantity']
                subtotal = operation['subtotal']
                
                year_month = created_at[:7]  
                
                if self.month_item_aggregations_by_client[client_id][year_month][item_id] is None:
                    self.month_item_aggregations_by_client[client_id][year_month][item_id] = ItemAggregation(item_id)
                
                self.month_item_aggregations_by_client[client_id][year_month][item_id].add_transaction(quantity, subtotal)

        except Exception as e:
            logger.error(f"Error aplicando operación BestSelling: {e}")
    
    def _serialize_client_data(self, client_id: str) -> Dict:
        """
        Serializa el estado de BestSelling incluyendo outgoing_counter.
        
        Estructura:
        {
            "aggregations": {
                "2024-01": {
                    "item_123": {
                        "sellings_qty": 150,
                        "profit_sum": 4500.50
                    }
                }
            },
            "outgoing_counter": 42
        }
        """
        client_aggregations = self.month_item_aggregations_by_client.get(client_id, {})
        serialized = {"aggregations": {}}
        
        for year_month, items_dict in client_aggregations.items():
            serialized["aggregations"][year_month] = {}
            
            for item_id, aggregation in items_dict.items():
                if aggregation is not None: 
                    serialized["aggregations"][year_month][item_id] = {
                        "sellings_qty": aggregation.sellings_qty,
                        "profit_sum": aggregation.profit_sum
                    }
        
        serialized["outgoing_counter"] = self.outgoing_counter_by_client.get(client_id, 0)
        
        logger.info(f"Serializando cliente {client_id}: counter={serialized['outgoing_counter']}")
        return serialized

    def _deserialize_client_data(self, client_id: str, data: Dict):
        """
        Reconstruye el estado de BestSelling desde un diccionario.
        """
        self.month_item_aggregations_by_client[client_id] = defaultdict(lambda: defaultdict(lambda: None))
        
        aggregations_data = data.get("aggregations", data)  
        
        for year_month, items_dict in aggregations_data.items():
            if year_month == "outgoing_counter": 
                continue
                
            for item_id, aggregation_data in items_dict.items():
                self.month_item_aggregations_by_client[client_id][year_month][item_id] = ItemAggregation(item_id)
                
                aggregation = self.month_item_aggregations_by_client[client_id][year_month][item_id]
                aggregation.sellings_qty = aggregation_data.get('sellings_qty', 0)
                aggregation.profit_sum = aggregation_data.get('profit_sum', 0.0)
        
        if "outgoing_counter" in data:
            self.outgoing_counter_by_client[client_id] = data["outgoing_counter"]
            logger.info(f"Contador restaurado para {client_id}: {data['outgoing_counter']}")
        else:
            self.outgoing_counter_by_client[client_id] = 0
            logger.info(f"Contador inicializado para {client_id}: 0 (checkpoint sin counter)")

    def clean_client_data(self, client_id: str):
        if client_id in self.month_item_aggregations_by_client:
            del self.month_item_aggregations_by_client[client_id]
            logger.info(f"Estado BestSelling limpiado para cliente {client_id}")
        else:
            logger.info(f"No se encontró estado BestSelling para limpiar del cliente {client_id}")

    def clean_all_data(self):
        self.month_item_aggregations_by_client.clear()
        logger.info("Estado BestSelling limpiado para todos los clientes")