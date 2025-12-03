from collections import defaultdict
import logging
from typing import Dict, Any, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TopCustomersAggregatorUtils:
    OP_START_TX = 'START_TRANSACTION'
    OP_COMMIT_TX = 'COMMIT_TRANSACTION'
    OP_DATA = 'DATA'
    OP_EOF = 'EOF'
    OP_SENT = 'SENT'
    OP_COUNTER = 'COUNTER'

    def __init__(self, aggregator_node):
        self.node = aggregator_node

    def serialize_operation(self, client_id: str, csv_line: str) -> Optional[str]:
        """
        Serializa operaciones para el WAL.
        """
        try:
            if any(op in csv_line for op in [self.OP_START_TX, self.OP_COMMIT_TX, self.OP_SENT, 'EOF:']):
                if csv_line.startswith(f"{client_id},"):
                    return csv_line
                return f"{client_id},{csv_line}"

            parts = csv_line.split(',')
            
            if len(parts) < 3 or parts[0] == 'store_id':
                return None
            
            store_id, user_id, purchases_qty = parts[0], parts[1], parts[2]
            
            if not all([store_id, user_id, purchases_qty]):
                return None
            
            return f"{client_id},{self.OP_DATA},{store_id},{user_id},{purchases_qty}"
            
        except Exception as e:
            logger.error(f"Error serializando operación: {e}")
            return None

    def deserialize_operation(self, operation_str: str) -> Dict[str, Any]:
        """
        Deserializa strings del WAL a diccionarios tipados.
        """
        try:
            parts = operation_str.split(',', 2)
            if len(parts) < 2:
                raise ValueError(f"Formato inválido: {operation_str}")
            
            client_id = parts[0]
            op_part = parts[1]
            content = parts[2] if len(parts) > 2 else ""
            
            if ':' in op_part:
                op_type, extra = op_part.split(':', 1)
                if not content: content = extra
            else:
                op_type = op_part

            if op_type == self.OP_START_TX:
                if ',' in content:
                    cid, query = content.split(',', 1)
                    return {'client_id': client_id, 'type': self.OP_START_TX, 'id': int(cid), 'query': query}
                return {'client_id': client_id, 'type': self.OP_START_TX, 'id': int(content)}

            if op_type == self.OP_COMMIT_TX:
                return {'client_id': client_id, 'type': self.OP_COMMIT_TX, 'id': int(content)}

            if op_type == self.OP_EOF:
                return {'client_id': client_id, 'type': self.OP_EOF, 'routing_key': content}
            
            if op_type == self.OP_SENT:
                return {'client_id': client_id, 'type': self.OP_SENT, 'query': content}

            if op_type == self.OP_DATA:
                data_parts = content.split(',')
                if len(data_parts) != 3:
                    raise ValueError(f"Datos incompletos: {content}")
                return {
                    'client_id': client_id, 'type': self.OP_DATA,
                    'store_id': data_parts[0], 'user_id': data_parts[1], 'qty': int(data_parts[2])
                }
            
            if op_type == self.OP_COUNTER:
                return {'client_id': client_id, 'type': self.OP_COUNTER, 'value': int(content)}

            raise ValueError(f"Opcode desconocido: {op_type}")

        except Exception as e:
            logger.error(f"Error deserializando: {e}")
            raise

    def apply_operation(self, operation: Dict[str, Any]):
        """
        Aplica evento al estado (Event Sourcing) + Lógica de Rollback.
        """
        try:
            client_id = operation['client_id']
            op_type = operation['type']
            
            if op_type == self.OP_START_TX:
                full_id = operation['id']
                counter_val = full_id % 1_000_000
                
                if client_id not in self.node.pending_rollbacks:
                    self.node.pending_rollbacks[client_id] = counter_val - 1
                    logger.debug(f"[WAL] Start Tx. Rollback point: {counter_val - 1}")
                return

            if op_type == self.OP_COMMIT_TX:
                full_id = operation['id']
                counter_val = full_id % 1_000_000
                self.node.outgoing_counter_by_client[client_id] = counter_val
                return

            if op_type == self.OP_SENT:
                if client_id in self.node.pending_rollbacks:
                    del self.node.pending_rollbacks[client_id]
                
                self.node.client_states[client_id] = self.node.ClientState.DATA_SENT
                return

            if op_type == self.OP_DATA:
                self.node.store_user_purchases_by_client[client_id][operation['store_id']][operation['user_id']] += operation['qty']
            
            elif op_type == self.OP_COUNTER:
                self.node.outgoing_counter_by_client[client_id] = operation['value']

        except Exception as e:
            logger.error(f"Error aplicando operación: {e}")

    def _serialize_client_data(self, client_id: str) -> Dict:
        """Serializa snapshot."""
        state_enum = self.node.client_states.get(client_id, self.node.ClientState.RECEIVING_DATA)
        
        serialized = {
            "stores": {},
            "client_state": state_enum.value,
            "outgoing_counter": self.node.outgoing_counter_by_client.get(client_id, 0)
        }
        
        client_data = self.node.store_user_purchases_by_client.get(client_id, {})
        for store_id, users in client_data.items():
            serialized["stores"][store_id] = dict(users)
        
        return serialized

    def _deserialize_client_data(self, client_id: str, data: Dict):
        """Restaura snapshot."""
        self.node.outgoing_counter_by_client[client_id] = data.get("outgoing_counter", 0)
        
        state_str = data.get("client_state", "receiving_data")
        try:
            self.node.client_states[client_id] = self.node.ClientState(state_str)
        except ValueError:
            self.node.client_states[client_id] = self.node.ClientState.RECEIVING_DATA
            
        stores_data = data.get("stores", {})
        self.node.store_user_purchases_by_client[client_id] = defaultdict(lambda: defaultdict(int))
        
        for store_id, users in stores_data.items():
            for user_id, count in users.items():
                self.node.store_user_purchases_by_client[client_id][store_id][user_id] = count
                
    def process_csv_line(self, csv_line: str, client_id: str):
        try:
            parts = csv_line.split(',')
            if len(parts) < 3 or parts[0] == 'store_id':
                return
            
            store_id = parts[0]
            user_id = parts[1]
            purchases_qty = int(parts[2])
            
            self.store_user_purchases_by_client[client_id][store_id][user_id] += purchases_qty
            
        except (ValueError, IndexError) as e:
            logger.warning(f"Error procesando línea: {csv_line}, error: {e}")

    def generate_top3_by_store(self, client_id: str) -> Dict[str, list]:
        top_3_by_store = {}
        
        client_data = self.store_user_purchases_by_client.get(client_id, {})
        
        for store_id in sorted(client_data.keys()):
            user_purchases = client_data[store_id]
            
            sorted_users = sorted(
                user_purchases.items(),
                key=lambda x: (-x[1], int(float(x[0].replace('.0', '')))),
            )
            
            top_3 = sorted_users[:3]
            top_3_by_store[store_id] = top_3
        
        logger.info(f"Top 3 calculado para cliente {client_id}: {len(top_3_by_store)} stores")
        return top_3_by_store