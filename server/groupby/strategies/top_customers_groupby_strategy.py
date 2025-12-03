import json
import logging
from collections import defaultdict
from typing import Dict
from .base_strategy import GroupByStrategy
from .user_purchase_count import UserPurchaseCount
import tempfile
import os

logger = logging.getLogger(__name__)


class TopCustomersGroupByStrategy(GroupByStrategy):
    def __init__(self, input_queue_name: str,outgoing_counter_by_client: Dict[str, int] = None):
        super().__init__()
        self.input_queue_name = input_queue_name
        self.store_user_purchases_by_client: Dict[str, Dict[str, Dict[str, UserPurchaseCount]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(UserPurchaseCount))
        )
        logger.info(f"TopCustomersGroupByStrategy inicializada para queue {input_queue_name}")
        self.outgoing_counter_by_client = outgoing_counter_by_client or defaultdict(int)
    def process_csv_line(self, csv_line: str, client_id: str = 'default_client'):
        try:
            store_id = self.dto_helper.get_column_value(csv_line, 'store_id')
            user_id = self.dto_helper.get_column_value(csv_line, 'user_id')
            if not store_id or not user_id or user_id.strip() == '':
                return
            
            if user_id not in self.store_user_purchases_by_client[client_id][store_id]:
                self.store_user_purchases_by_client[client_id][store_id][user_id] = UserPurchaseCount(user_id)
            
            self.store_user_purchases_by_client[client_id][store_id][user_id].add_purchase()
            
        except (ValueError, IndexError) as e:
            logger.warning(f"Error procesando línea: {e}")


    def generate_results_csv_for_client(self, client_id: str) -> str:
        client_data = self.store_user_purchases_by_client.get(client_id, {})
        csv_lines = []
        for store_id in sorted(client_data.keys()):
            store_csv_lines = ["store_id,user_id,purchases_qty"]
            user_purchases = client_data[store_id]
            for user_purchase in user_purchases.values():
                store_csv_lines.append(user_purchase.to_csv_line(store_id))
            csv_lines.extend(store_csv_lines)
        return '\n'.join(csv_lines)
    
 
    def serialize_operation(self, client_id: str, csv_line: str) -> str:
        """
        Serializa una operación TopCustomers para el WAL.
        
        Formato: client_id,store_id,user_id
        Ejemplo: 0,store_1,user_123
        """
        try:
            if csv_line.startswith(f'{client_id},COUNTER,'):
                parts = csv_line.split(',')
                counter_value = parts[2]
                return f"{client_id},COUNTER,{counter_value}"
            store_id = self.dto_helper.get_column_value(csv_line, 'store_id')
            user_id = self.dto_helper.get_column_value(csv_line, 'user_id')
            
            if not store_id or not user_id or user_id.strip() == '':
                return None
            
            return f"{client_id},{store_id},{user_id}"
            
        except Exception as e:
            logger.error(f"Error serializando operación TopCustomers: {e}")
            raise

    def deserialize_operation(self, operation_str: str) -> dict:
        """
        Deserializa una operación TopCustomers desde el WAL.
        
        Input: "0,store_1,user_123"
            "0,COUNTER,5"
        """
        try:
            parts = operation_str.split(',')
            
            if len(parts) < 3:
                raise ValueError(f"Formato inválido: {operation_str}")
            
            client_id = parts[0]
            
            if parts[1] == 'COUNTER':
                counter_value = int(parts[2])
                return {
                    'client_id': client_id,
                    'type': 'counter',
                    'counter_value': counter_value
                }
            
            if len(parts) != 3:
                raise ValueError(f"Formato inválido, esperaba 3 campos, encontró {len(parts)}: {operation_str}")
            
            client_id, store_id, user_id = parts
            
            return {
                'client_id': client_id,
                'type': 'data',
                'store_id': store_id,
                'user_id': user_id
            }
            
        except Exception as e:
            logger.error(f"Error deserializando operación TopCustomers: {e}")
            raise

    def apply_operation(self, operation: dict):
        """
        Aplica una operación TopCustomers al estado en memoria.
        """
        try:
            client_id = operation['client_id']
            op_type = operation.get('type', 'data')  
            
            if op_type == 'counter':
                counter_value = operation['counter_value']
                self.outgoing_counter_by_client[client_id] = counter_value
                logger.debug(f"[WAL Recovery] Contador restaurado: {counter_value} para cliente {client_id}")
                return
            
            store_id = operation['store_id']
            user_id = operation['user_id']
            
            if not user_id or user_id.strip() == '':
                return
            
            if user_id not in self.store_user_purchases_by_client[client_id][store_id]:
                self.store_user_purchases_by_client[client_id][store_id][user_id] = UserPurchaseCount(user_id)
            
            self.store_user_purchases_by_client[client_id][store_id][user_id].add_purchase()
            
        except Exception as e:
            logger.error(f"Error aplicando operación TopCustomers: {e}")
            raise
    
    def _serialize_client_data(self, client_id: str) -> Dict:
        """
        Serializa el estado de TopCustomers a un diccionario.
        """
        client_data = self.store_user_purchases_by_client.get(client_id, {})
        serialized = {
            'stores': {},
            'outgoing_counter': self.outgoing_counter_by_client.get(client_id, 0) 
        }
        
        for store_id, users in client_data.items():
            serialized['stores'][store_id] = {}
            for user_id, user_purchase in users.items():
                serialized['stores'][store_id][user_id] = user_purchase.purchases_qty
        
        logger.info(f"Guardando contador saliente TopCustomers: {serialized['outgoing_counter']} para cliente {client_id}")
        return serialized

    def _deserialize_client_data(self, client_id: str, data: Dict):
        """
        Reconstruye el estado de TopCustomers desde un diccionario.
        
        Args:
            client_id: ID del cliente
            data: Diccionario con estructura {
                'stores': {store_id: {user_id: count}},
                'outgoing_counter': counter_value
            }
        """
        if 'outgoing_counter' in data:
            self.outgoing_counter_by_client[client_id] = data['outgoing_counter']
            logger.info(f"Restaurado contador saliente TopCustomers: {data['outgoing_counter']} para cliente {client_id}")
        else:
            self.outgoing_counter_by_client[client_id] = 0
            logger.warning(f"No se encontró outgoing_counter para cliente {client_id}, inicializando en 0")
        
        self.store_user_purchases_by_client[client_id] = defaultdict(lambda: defaultdict(UserPurchaseCount))
        
        stores_data = data.get('stores', {})
        for store_id, users in stores_data.items():
            for user_id, count in users.items():
                self.store_user_purchases_by_client[client_id][store_id][user_id] = UserPurchaseCount(user_id)
                self.store_user_purchases_by_client[client_id][store_id][user_id].purchases_qty = count
        
        logger.info(f"Restaurado estado TopCustomers: {len(stores_data)} stores para cliente {client_id}")
        
    def clean_client_data(self, client_id: str):
        if client_id in self.store_user_purchases_by_client:
            del self.store_user_purchases_by_client[client_id]
            logger.info(f"Estado TopCustomers limpiado para cliente {client_id}")
        else:
            logger.info(f"No se encontró estado TopCustomers para limpiar del cliente {client_id}")