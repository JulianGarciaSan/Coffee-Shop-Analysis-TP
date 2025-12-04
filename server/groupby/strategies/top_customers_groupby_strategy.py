import json
import logging
from collections import defaultdict
from typing import Any, Dict, Optional
from .base_strategy import GroupByStrategy
from .user_purchase_count import UserPurchaseCount
import tempfile
import os

logger = logging.getLogger(__name__)


class TopCustomersGroupByStrategy(GroupByStrategy):
    OP_START_TX = 'START_TRANSACTION'
    OP_COMMIT_TX = 'COMMIT_TRANSACTION'
    OP_SENT = 'SENT'
    OP_EOF = 'EOF'
    OP_DATA = 'DATA'
    OP_COUNTER = 'COUNTER'
    
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
    
 
    def serialize_operation(self, client_id: str, csv_line: str) -> Optional[str]:
        """
        Serializa operaciones para el WAL.
        Distingue entre operaciones de control (ya formateadas) y datos nuevos.
        """
        try:
            # 1. Operaciones de Control (Start, Commit, Sent, EOF, Counter)
            # Si la línea ya contiene uno de estos opcodes, asumimos que viene del sistema
            if any(op in csv_line for op in [self.OP_START_TX, self.OP_COMMIT_TX, self.OP_SENT, self.OP_EOF, 'PENDING_ID', 'COMMIT_ID']):
                if csv_line.startswith(f"{client_id},"):
                    return csv_line
                return f"{client_id},{csv_line}"

            # 2. Datos de Negocio (CSV Raw)
            # Formato esperado: store_id, user_id, purchases_qty
            store_id = self.dto_helper.get_column_value(csv_line, 'store_id')
            user_id = self.dto_helper.get_column_value(csv_line, 'user_id')
            # Nota: purchases_qty no siempre está en el CSV raw de entrada (depende de la etapa),
            # pero asumimos que procesamos lo que sea necesario para reconstruir el estado.
            # En TopCustomers, usualmente contamos ocurrencias (+1) o sumamos qty si viene.
            # Para simplificar y seguir tu lógica de "process_csv_line", aquí asumimos que reconstruimos desde cero.
            
            if not store_id or not user_id:
                return None
            
            # Formato Estandarizado: client_id, DATA, store_id, user_id
            return f"{client_id},{self.OP_DATA},{store_id},{user_id}"
            
        except Exception as e:
            logger.error(f"Error serializando operación TopCustomers: {e}")
            return None

    def deserialize_operation(self, operation_str: str) -> Dict[str, Any]:
        """
        Deserializa operaciones del WAL a diccionarios tipados.
        Maneja robustamente separadores y formatos.
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

            # Manejo de separadores ':' (ej: EOF:top_customers)
            if ':' in op_part:
                op_type, extra = op_part.split(':', 1)
                if not content: content = extra
            else:
                op_type = op_part

            # --- Transacciones y Control ---
            if op_type == self.OP_START_TX:
                # Content: "ID, Query" o "ID"
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

            # --- Datos de Negocio ---
            if op_type == self.OP_DATA:
                # Content: store_id, user_id
                data_parts = content.split(',')
                if len(data_parts) < 2:
                    raise ValueError(f"Datos incompletos para DATA: {content}")
                
                return {
                    'client_id': client_id,
                    'type': self.OP_DATA,
                    'store_id': data_parts[0],
                    'user_id': data_parts[1]
                }

            # Fallback para compatibilidad con logs viejos (sin OP_DATA explícito)
            # Si llegamos acá y parece dato (3 partes total), lo tratamos como DATA
            if len(parts) == 3 and op_type not in [self.OP_START_TX, self.OP_COMMIT_TX]:
                 return {
                    'client_id': client_id,
                    'type': self.OP_DATA,
                    'store_id': parts[1],
                    'user_id': parts[2]
                }

            raise ValueError(f"Opcode desconocido: {op_type}")

        except Exception as e:
            logger.error(f"Error deserializando '{operation_str}': {e}")
            raise

    def apply_operation(self, operation: Dict[str, Any]):
        """
        Aplica la operación al estado en memoria.
        Ignora operaciones de sistema (Start/Commit/Sent) ya que esas 
        son para el CheckpointHandler/Main.
        """
        try:
            client_id = operation['client_id']
            op_type = operation.get('type')
            
            # 1. Operaciones de Sistema -> No afectan el modelo de negocio (Purchases)
            if op_type in [self.OP_START_TX, self.OP_COMMIT_TX, self.OP_SENT, self.OP_EOF, self.OP_COUNTER]:
                # Opcional: Si quisieras restaurar el contador aquí, podrías:
                # if op_type == self.OP_COUNTER: self.outgoing_counter_by_client[client_id] = operation['value']
                return

            # 2. Operaciones de Datos
            if op_type == self.OP_DATA:
                store_id = operation['store_id']
                user_id = operation['user_id']
                
                if not user_id or not store_id: return
                
                if user_id not in self.store_user_purchases_by_client[client_id][store_id]:
                    self.store_user_purchases_by_client[client_id][store_id][user_id] = UserPurchaseCount(user_id)
                
                # En TopCustomers, cada línea es una compra (+1)
                self.store_user_purchases_by_client[client_id][store_id][user_id].add_purchase()

        except Exception as e:
            logger.error(f"Error aplicando operación TopCustomers: {e}")
    
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
            
    def clean_all_data(self):
        self.store_user_purchases_by_client.clear()
        logger.info("Estado TopCustomers limpiado para TODOS los clientes")