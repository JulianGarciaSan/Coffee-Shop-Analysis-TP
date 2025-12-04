from collections import defaultdict
import logging
from typing import Dict, Any, Optional, Tuple

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BestSellingAggregatorUtils:
    OP_START_TX = 'START_TRANSACTION'
    OP_COMMIT_TX = 'COMMIT_TRANSACTION'
    OP_SELLING = 'S'
    OP_PROFIT = 'P'
    OP_EOF = 'EOF'
    OP_SENT = 'SENT'

    def __init__(self, aggregator_node):
        self.node = aggregator_node

    def serialize_operation(self, client_id: str, csv_line: str) -> Optional[str]:
        try:
            if any(op in csv_line for op in [self.OP_START_TX, self.OP_COMMIT_TX, self.OP_EOF, self.OP_SENT]):
                if csv_line.startswith(f"{client_id},"):
                    return csv_line
                return f"{client_id},{csv_line}"

            parts = csv_line.split(',')
            
            if len(parts) < 3 or parts[0] == 'created_at':
                return None
            
            year_month = parts[0].strip()
            item_id = parts[1].strip()
            value_str = parts[2].strip()
            
            try:
                if '.' not in value_str or value_str.endswith('.0'):
                    value = int(float(value_str))
                    return f"{client_id},{self.OP_SELLING},{year_month},{item_id},{value}"
                else:
                    value = float(value_str)
                    return f"{client_id},{self.OP_PROFIT},{year_month},{item_id},{value}"
            except ValueError:
                logger.warning(f"Error parseando valor numérico: '{value_str}'")
                return None
            
        except Exception as e:
            logger.error(f"Error serializando operación: {e}")
            return None

    def deserialize_operation(self, operation_str: str) -> Dict[str, Any]:
        try:
            operation_str = operation_str.strip()
            if not operation_str:
                raise ValueError("Línea vacía")

            parts = operation_str.split(',', 2)
            if len(parts) < 2:
                raise ValueError(f"Formato inválido: {operation_str}")
            
            client_id = parts[0]
            raw_op_part = parts[1].strip()

            op_type = raw_op_part
            content = parts[2] if len(parts) > 2 else ""

            if ':' in raw_op_part and (raw_op_part.startswith('EOF') or raw_op_part.startswith('SENT')):
                op_type, extra_content = raw_op_part.split(':', 1)
                content = extra_content 
            
            if op_type == self.OP_START_TX:
                if ',' in content:
                    msg_id_str, query = content.split(',', 1)
                    return {'client_id': client_id, 'type': self.OP_START_TX, 'id': int(msg_id_str.strip()), 'query': query.strip()}
                return {'client_id': client_id, 'type': self.OP_START_TX, 'id': int(content.strip())}

            if op_type == self.OP_COMMIT_TX:
                return {'client_id': client_id, 'type': self.OP_COMMIT_TX, 'id': int(content.strip())}

            if op_type == self.OP_EOF:
                return {'client_id': client_id, 'type': self.OP_EOF, 'routing_key': content.strip()}
            
            if op_type == self.OP_SENT:
                return {'client_id': client_id, 'type': self.OP_SENT, 'query': content.strip()}

            if op_type in [self.OP_SELLING, self.OP_PROFIT]:
                data_parts = content.split(',')
                if len(data_parts) != 3:
                     raise ValueError(f"Datos incompletos para {op_type}: {content}")

                year_month = data_parts[0].strip()
                item_id = data_parts[1].strip()
                val_str = data_parts[2].strip()
                
                if op_type == self.OP_SELLING:
                    return {
                        'client_id': client_id, 'type': self.OP_SELLING,
                        'year_month': year_month, 'item_id': item_id, 'value': int(val_str)
                    }
                else:
                    return {
                        'client_id': client_id, 'type': self.OP_PROFIT,
                        'year_month': year_month, 'item_id': item_id, 'value': float(val_str)
                    }
            
            raise ValueError(f"Opcode desconocido: {op_type}")

        except Exception as e:
            logger.error(f"Error deserializando '{operation_str}': {e}")
            raise

    def apply_operation(self, operation: Dict[str, Any]):

        client_id = operation['client_id']
        op_type = operation['type']
        
        if op_type == self.OP_START_TX:
            full_id = operation['id']
            counter_val = full_id % 1_000_000
            if client_id not in self.node.pending_rollbacks:
                self.node.pending_rollbacks[client_id] = counter_val - 1
            return

        if op_type == self.OP_COMMIT_TX:
            full_id = operation['id']
            counter_val = full_id % 1_000_000
            self.node.outgoing_counter_by_client[client_id] = counter_val
            return

        if op_type == self.OP_SENT:
            if client_id in self.node.pending_rollbacks:
                del self.node.pending_rollbacks[client_id]
            return

        if op_type == self.OP_EOF:
            routing_key = operation['routing_key']
            if 'top_selling' in routing_key:
                self.node.eof_selling_count_by_client[client_id] += 1
            elif 'top_profit' in routing_key:
                self.node.eof_profit_count_by_client[client_id] += 1
        
        elif op_type == self.OP_SELLING:
            self.node.month_selling_candidates_by_client[client_id][operation['year_month']].append({
                'item_id': operation['item_id'], 'sellings_qty': operation['value']
            })
            
        elif op_type == self.OP_PROFIT:
            self.node.month_profit_candidates_by_client[client_id][operation['year_month']].append({
                'item_id': operation['item_id'], 'profit_sum': operation['value']
            })

    def _serialize_client_data(self, client_id: str) -> Dict:
        return {
            "selling_candidates": self.node.month_selling_candidates_by_client.get(client_id, {}),
            "profit_candidates": self.node.month_profit_candidates_by_client.get(client_id, {}),
            "eof_selling_count": self.node.eof_selling_count_by_client.get(client_id, 0),
            "eof_profit_count": self.node.eof_profit_count_by_client.get(client_id, 0),
            "outgoing_counter": self.node.outgoing_counter_by_client.get(client_id, 0)
        }

    def _deserialize_client_data(self, client_id: str, data: Dict):
        self.node.month_selling_candidates_by_client[client_id] = defaultdict(list, data.get("selling_candidates", {}))
        self.node.month_profit_candidates_by_client[client_id] = defaultdict(list, data.get("profit_candidates", {}))
        self.node.eof_selling_count_by_client[client_id] = data.get("eof_selling_count", 0)
        self.node.eof_profit_count_by_client[client_id] = data.get("eof_profit_count", 0)
        self.node.outgoing_counter_by_client[client_id] = data.get("outgoing_counter", 0)
        
            
    def process_csv_line(self, csv_line: str, routing_key: str, client_id: str):
        try:
            parts = csv_line.split(',')
            if len(parts) < 3 or parts[0] == 'created_at':
                return
            
            year_month = parts[0]
            item_id = parts[1]
            
            if 'top_selling' in routing_key:
                sellings_qty = int(parts[2])
                
                self.node.month_selling_candidates_by_client[client_id][year_month].append({
                    'item_id': item_id,
                    'sellings_qty': sellings_qty
                })
                                
            elif 'top_profit' in routing_key:
                profit_sum = float(parts[2])
                
                self.node.month_profit_candidates_by_client[client_id][year_month].append({
                    'item_id': item_id,
                    'profit_sum': profit_sum
                })
                            
        except (ValueError, IndexError) as e:
            logger.warning(f"Error procesando línea: {csv_line}, error: {e}")
    
    def calculate_global_top1(self, client_id: str) -> Tuple[Dict, Dict]:
        best_selling = {}
        most_profit = {}
        
        selling_candidates = self.node.month_selling_candidates_by_client.get(client_id, {})
        for year_month, candidates in selling_candidates.items():
            if not candidates:
                continue            
            top = max(candidates, 
                    key=lambda x: (x['sellings_qty'], -int(x['item_id']) if x['item_id'].isdigit() else 0))
            
            best_selling[year_month] = (top['item_id'], top['sellings_qty'])
        
        profit_candidates = self.node.month_profit_candidates_by_client.get(client_id, {})
        for year_month, candidates in profit_candidates.items():
            if not candidates:
                continue

            top = max(candidates,
                    key=lambda x: (x['profit_sum'], -int(x['item_id']) if x['item_id'].isdigit() else 0))
            
            most_profit[year_month] = (top['item_id'], top['profit_sum'])
        
        return best_selling, most_profit
    
    def generate_top1_csv(self, top_dict: Dict[str, Tuple[str, float]], metric_name: str) -> str:
        csv_lines = [f"created_at,item_id,{metric_name}"]
        
        for year_month in sorted(top_dict.keys()):
            item_id, value = top_dict[year_month]
            if metric_name == "sellings_qty":
                csv_lines.append(f"{year_month},{item_id},{int(value)}")
            else:
                csv_lines.append(f"{year_month},{item_id},{value:.1f}")
        
        return '\n'.join(csv_lines)