import logging
import os
import sys
import time
from typing import Dict
from collections import defaultdict
from rabbitmq.middleware import MessageMiddlewareExchange, MessageMiddlewareQueueManual
from dtos.dto import TransactionBatchDTO, BatchType
from common.graceful_shutdown import GracefulShutdown
from logger_monitor.logger_monitor import LoggerMonitor
from healthchecker.healthchecker import HealthChecker
from checkpoint_handler.checkpoint_handler import CheckpointHandler


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TopCustomersAggregatorNode:
    def __init__(self):
        self.shutdown = GracefulShutdown()
        self.shutdown.register_callback(self._on_shutdown_signal)
        
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.node_id = os.getenv('TOPK_NODE_ID', '1')
        self.total_nodes = int(os.getenv('TOTAL_TOPK_NODES', '2'))
        self.total_join_nodes = int(os.getenv('TOTAL_JOIN_NODES', '2'))
        self.checkpoint_dir = os.getenv('CHECKPOINT_DIR', f'/app/server/logs/groupby_top_customers/checkpoints')

        
        total_stores = 10
        stores_per_node = total_stores // self.total_nodes
        extra_stores = total_stores % self.total_nodes
        
        self.client_logger = LoggerMonitor('/app/client_logs.txt')
        
        self.message_id = 0
        
        self.is_first_message = True
        
        try:
            node_num = int(self.node_id)
        except ValueError:
            node_num = int(str(self.node_id).split('_')[-1])
        
        start_store = (node_num - 1) * stores_per_node + min(node_num - 1, extra_stores)
        end_store = start_store + stores_per_node + (1 if node_num <= extra_stores else 0)
        
        self.expected_eof_per_client = end_store - start_store
        
        self.store_user_purchases_by_client: Dict[str, Dict[str, Dict[str, int]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(int))
        )
        self.eof_count_by_client: Dict[str, int] = defaultdict(int)
        
        health_port = int(os.getenv('HEALTH_PORT', '9999'))
        self.health_server = HealthChecker(port=health_port)
        self.health_server.start()
        
        self.checkpoint_handler = CheckpointHandler(
            checkpoint_dir=self.checkpoint_dir,
            strategy=self,
        )
        
        self._setup_input_middleware(start_store, end_store)
        self._setup_output_middleware()
        
        if hasattr(self.input_middleware, 'shutdown'):
            self.input_middleware.shutdown = self.shutdown
        if hasattr(self.output_middleware, 'shutdown'):
            self.output_middleware.shutdown = self.shutdown
        
        logger.info(f"TopCustomersAggregatorNode {self.node_id} inicializado")
        logger.info(f"  Procesa stores: {start_store + 1}-{end_store}")
        logger.info(f"  Espera {self.expected_eof_per_client} EOF por cliente")
        logger.info(f"  Enviará a {self.total_join_nodes} join nodes")
    
    def _setup_input_middleware(self, start_store: int, end_store: int):
        input_exchange = os.getenv('INPUT_EXCHANGE', 'aggregated.exchange')
        queue_name = os.getenv('INPUT_QUEUE', f'aggregated_data_{self.node_id}')
        
        routing_keys = [f"store.{i}" for i in range(start_store + 1, end_store + 1)]
        
        self.input_middleware = MessageMiddlewareQueueManual(
            host=self.rabbitmq_host,
            queue_name=queue_name,
            exchange_name=input_exchange,
            routing_keys=routing_keys
        )
        
        self.input_middleware.shutdown = self.shutdown
        
        logger.info(f"  Input: {input_exchange}, routing keys: {routing_keys}")
    
    def _setup_output_middleware(self):
        output_exchange = os.getenv('OUTPUT_EXCHANGE', 'join.exchange')
        
        route_keys = [f'join_node_{i}.top_customers.data' for i in range(self.total_join_nodes)]
        
        self.output_middleware = MessageMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name=output_exchange,
            route_keys=route_keys
        )
        
        self.output_middleware.shutdown = self.shutdown
        
        logger.info(f"  Output: {output_exchange}, routing keys: {route_keys}")
    
    def _on_shutdown_signal(self):
        logger.info("Señal de shutdown recibida")
        if self.input_middleware:
            self.input_middleware.stop_consuming()
            
    def _generate_next_message_id(self, client_id: str) -> str:
        self.message_id += 1
        return f"{client_id}_{self.message_id}:TC"

    
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
    
    def send_sharded_to_join_nodes(self, top_35_by_store: Dict[str, list], client_id: str):
        batches_by_node = {i: [] for i in range(self.total_join_nodes)}
        
        for store_id, top_users in top_35_by_store.items():
            for user_id, purchases_qty in top_users:
                clean_id = user_id.rstrip('.0') if user_id.endswith('.0') else user_id
                shard_id = int(clean_id) % self.total_join_nodes
                
                batches_by_node[shard_id].append({
                    'store_id': store_id,
                    'user_id': user_id,  
                    'purchases_qty': purchases_qty
                })
        
        for node_id, batch in batches_by_node.items():
            if not batch:
                continue
            
            csv_lines = ['store_id,user_id,purchases_qty']
            for record in batch:
                csv_lines.append(f"{record['store_id']},{record['user_id']},{record['purchases_qty']}")
            
            csv_data = '\n'.join(csv_lines)
            self.send_data_to_join_node(csv_data, client_id, node_id)
                    
        for node_id in range(self.total_join_nodes):
            self.send_eof_to_join_node(client_id, node_id)
        
        logger.info(f"EOF enviado a {self.total_join_nodes} join nodes para cliente {client_id}")

    def send_data_to_join_node(self, csv_data: str, client_id: str, node_id: int):
        print("///////////// ENVIANDO DATA A JOIN NODE /////////////")
        time.sleep(10)
        new_message_id = self._generate_next_message_id(client_id)
        result_dto = TransactionBatchDTO(csv_data, BatchType.RAW_CSV)
        routing_key = f"join_node_{node_id}.top_customers.data"
        
        self.output_middleware.send(
            result_dto.to_bytes_fast(),
            routing_key=routing_key,
            headers={'client_id': client_id, 'message_id': new_message_id}
        )
    def send_eof_to_join_node(self, client_id: str, node_id: int):
        print("///////////// ENVIANDO EOF A JOIN NODE /////////////")
        time.sleep(10)
        new_message_id = self._generate_next_message_id(client_id)
        eof_dto = TransactionBatchDTO(f"EOF:{client_id}", BatchType.EOF)
        routing_key = f"join_node_{node_id}.top_customers.data"
        
        self.output_middleware.send(
            eof_dto.to_bytes_fast(),
            routing_key=routing_key,
            headers={'client_id': client_id, 'message_id': new_message_id}
        )
    def handle_eof(self, dto: TransactionBatchDTO, client_id: str) -> bool:
        
        self.eof_count_by_client[client_id] += 1
        logger.info(f"EOF {self.eof_count_by_client[client_id]}/{self.expected_eof_per_client} "
                   f"para cliente {client_id}")
        
        if self.eof_count_by_client[client_id] >= self.expected_eof_per_client:
            logger.info(f"EOF completo para cliente {client_id} - calculando Top 3")
            
            top_3_results = self.generate_top3_by_store(client_id)
            self.send_sharded_to_join_nodes(top_3_results, client_id)
            
            if client_id in self.store_user_purchases_by_client:
                del self.store_user_purchases_by_client[client_id]
            self.eof_count_by_client[client_id] = 0
            
            logger.info(f"Cliente {client_id} completado y limpiado")
        
        return False

    def process_message(self, message: bytes,client_id: str = None, message_id: str = None) -> bool:
        try:
            if self.shutdown.is_shutting_down():
                return (True, False)
            
            dto = TransactionBatchDTO.from_bytes_fast(message)
                    
            if dto.batch_type == BatchType.EOF:
                self.handle_eof(dto, client_id)
                self.checkpoint_handler.save_eof_checkpoint(client_id, message_id)
                return (False, True) 
                # return self.handle_eof(dto, routing_key)
            
            if dto.batch_type == BatchType.RAW_CSV:
                lines = [line.strip() for line in dto.data.split('\n') if line.strip()]
                
                for i, line in enumerate(lines):
                    try:
                        self.process_csv_line(line, client_id)
                    except Exception as e:
                        logger.warning(f"Línea {i} inválida en mensaje {message_id}: {e}")
                
                self.checkpoint_handler.save_message_checkpoint(
                    client_id=client_id,
                    message_id=message_id,
                    csv_lines=lines
                )
                
                return (False, True)
            
        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")
            return False
        
    def parse_message_headers(self, properties):
        client_id = None
        message_id = None
        if properties and properties.headers:
            client_id = properties.headers.get('client_id')
            message_id = properties.headers.get('message_id')
        return str(client_id), str(message_id)
    
    
    def on_message_callback(self, ch, method, properties, body):
        try:
            if self.shutdown.is_shutting_down():
                ch.stop_consuming()
                return
 
            routing_key = getattr(method, 'routing_key', None)
            client_id, message_id = self.parse_message_headers(properties)
            
            if self.checkpoint_handler.analyze_first_message(client_id, message_id, ch, method, body):
                return
            
            self.checkpoint_handler.register_incoming_message(client_id, message_id)
            should_stop, should_ack = self.process_message(body, client_id, message_id)

            if should_ack:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                
            if should_stop:
                logger.info("Shutdown solicitado - deteniendo consuming")
                ch.stop_consuming()
        except Exception as e:
            logger.error(f"Error en callback: {e}")
    
    def start(self):
        try:
            logger.info(f"Iniciando TopCustomersAggregator {self.node_id}...")
            self.input_middleware.start_consuming(self.on_message_callback)
        except KeyboardInterrupt:
            logger.info("Detenido manualmente")
        except Exception as e:
            logger.error(f"Error: {e}")
            raise
        finally:
            self._cleanup()
    
    def _cleanup(self):
        try:
            if self.input_middleware:
                self.input_middleware.close()
            if self.output_middleware:
                self.output_middleware.close()
            logger.info("Conexiones cerradas")
        except Exception as e:
            logger.error(f"Error en cleanup: {e}")   
            
    def serialize_operation(self, client_id: str, csv_line: str) -> str:
        """
        Serializa una operación TopCustomersAggregator para el WAL.
        
        Formato: client_id,store_id,user_id,purchases_qty
        Ejemplo: 0,store_1,user_123,5
        """
        try:
            parts = csv_line.split(',')
            
            if len(parts) < 3 or parts[0] == 'store_id':
                return None
            
            store_id = parts[0]
            user_id = parts[1]
            purchases_qty = parts[2]
            
            if not all([store_id, user_id, purchases_qty]):
                logger.warning(f"Línea con datos incompletos, saltando")
                return None
            
            return f"{client_id},{store_id},{user_id},{purchases_qty}"
            
        except Exception as e:
            logger.error(f"Error serializando operación: {e}")
            raise

    def deserialize_operation(self, operation_str: str) -> dict:
        """
        Deserializa una operación desde el WAL.
        
        Input: "0,store_1,user_123,5"
        Output: {
            'client_id': '0',
            'store_id': 'store_1',
            'user_id': 'user_123',
            'purchases_qty': 5
        }
        """
        try:
            parts = operation_str.split(',')
            
            if len(parts) != 4:
                raise ValueError(f"Formato inválido, esperaba 4 campos, encontró {len(parts)}: {operation_str}")
            
            client_id, store_id, user_id, purchases_qty_str = parts
            
            return {
                'client_id': client_id,
                'store_id': store_id,
                'user_id': user_id,
                'purchases_qty': int(purchases_qty_str)
            }
            
        except Exception as e:
            logger.error(f"Error deserializando operación: {e}")
            raise

    def apply_operation(self, operation: dict):
        """
        Aplica una operación al estado en memoria.
        Similar a process_csv_line pero desde dict ya parseado.
        """
        try:
            client_id = operation['client_id']
            store_id = operation['store_id']
            user_id = operation['user_id']
            purchases_qty = operation['purchases_qty']
            
            self.store_user_purchases_by_client[client_id][store_id][user_id] += purchases_qty
            
        except Exception as e:
            logger.error(f"Error aplicando operación: {e}")
            raise

    def _serialize_client_data(self, client_id: str) -> Dict:
        """
        Serializa el estado completo del cliente para checkpoint.
        
        Estructura:
        {
            "stores": {
                "store_1": {
                    "user_123": 10,
                    "user_456": 5
                },
                "store_2": {
                    "user_789": 3
                }
            },
            "eof_count": 2,
            "message_counter": 42
        }
        """
        client_data = self.store_user_purchases_by_client.get(client_id, {})
        
        serialized = {
            "stores": {},
            "eof_count": self.eof_count_by_client.get(client_id, 0)
        }
        
        for store_id, users in client_data.items():
            serialized["stores"][store_id] = dict(users)
        
        if hasattr(self, '_message_counter') and client_id in self._message_counter:
            serialized["message_counter"] = self._message_counter[client_id]
        
        return serialized

    def _deserialize_client_data(self, client_id: str, data: Dict):
        """
        Reconstruye el estado desde un checkpoint.
        
        Args:
            client_id: ID del cliente
            data: Diccionario con estructura {stores: {...}, eof_count: N}
        """
        self.store_user_purchases_by_client[client_id] = defaultdict(lambda: defaultdict(int))
        
        stores_data = data.get("stores", {})
        for store_id, users in stores_data.items():
            for user_id, count in users.items():
                self.store_user_purchases_by_client[client_id][store_id][user_id] = count
        
        self.eof_count_by_client[client_id] = data.get("eof_count", 0)
        
        if "message_counter" in data:
            if not hasattr(self, '_message_counter'):
                self._message_counter = {}
            self._message_counter[client_id] = data["message_counter"]
            
    def _message_id_to_int(self, message_id: str) -> int:
        """
        Convierte message_id compuesto a entero único.

        """
        try:
            parts = message_id.split('_')
            if len(parts) == 2:
                node_id = int(parts[0])
                counter = int(parts[1])
                return node_id * 1000000 + counter
            else:
                return int(message_id)
        except Exception as e:
            logger.warning(f"Error parseando message_id {message_id}: {e}")
            return int(message_id.split('_')[-1])

if __name__ == "__main__":
    try:
        aggregator = TopCustomersAggregatorNode()
        aggregator.start()
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error fatal: {e}")
        sys.exit(1)