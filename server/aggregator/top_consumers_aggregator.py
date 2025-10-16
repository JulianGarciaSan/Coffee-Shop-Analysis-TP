import logging
import os
import sys
from typing import Dict
from collections import defaultdict
from rabbitmq.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
from dtos.dto import TransactionBatchDTO, BatchType
from common.graceful_shutdown import GracefulShutdown

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
        
        total_stores = 10
        stores_per_node = total_stores // self.total_nodes
        extra_stores = total_stores % self.total_nodes
        
        try:
            node_num = int(self.node_id)
        except ValueError:
            node_num = int(str(self.node_id).split('_')[-1])
        
        start_store = (node_num - 1) * stores_per_node + min(node_num - 1, extra_stores)
        end_store = start_store + stores_per_node + (1 if node_num <= extra_stores else 0)
        
        self.expected_eof_per_client = end_store - start_store
        
        self.store_user_purchases: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self.eof_count_by_client: Dict[str, int] = defaultdict(int)
        
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
        
        self.input_middleware = MessageMiddlewareQueue(
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
    
    def process_csv_line(self, csv_line: str):
        try:
            parts = csv_line.split(',')
            if len(parts) < 3 or parts[0] == 'store_id':
                return
            
            store_id = parts[0]
            user_id = parts[1]
            purchases_qty = int(parts[2])
            
            self.store_user_purchases[store_id][user_id] += purchases_qty
            
        except (ValueError, IndexError) as e:
            logger.warning(f"Error procesando línea: {csv_line}, error: {e}")

    def generate_top3_by_store(self) -> Dict[str, list]:
        top_3_by_store = {}
        
        for store_id in sorted(self.store_user_purchases.keys()):
            user_purchases = self.store_user_purchases[store_id]
            
            sorted_users = sorted(
                user_purchases.items(),
                key=lambda x: (-x[1], int(float(x[0].replace('.0', '')))),
            )
            
            top_3 = sorted_users[:3]  # TOP 3 en lugar de TOP 35
            top_3_by_store[store_id] = top_3
        
        logger.info(f"Top 3 calculado para {len(top_3_by_store)} stores")
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
            
            result_dto = TransactionBatchDTO(csv_data, BatchType.RAW_CSV)
            routing_key = f"join_node_{node_id}.top_customers.data"
            
            self.output_middleware.send(
                result_dto.to_bytes_fast(),
                routing_key=routing_key,
                headers={'client_id': client_id}
            )
            
            logger.info(f"Cliente {client_id} → join_node_{node_id}: {len(batch)} top_customers")
        
        for node_id in range(self.total_join_nodes):
            eof_dto = TransactionBatchDTO(f"EOF:{client_id}", BatchType.EOF)
            routing_key = f"join_node_{node_id}.top_customers.data"
            
            self.output_middleware.send(
                eof_dto.to_bytes_fast(),
                routing_key=routing_key,
                headers={'client_id': client_id}
            )
        
        logger.info(f"EOF enviado a {self.total_join_nodes} join nodes para cliente {client_id}")
    
    def handle_eof(self, dto: TransactionBatchDTO, routing_key: str = None) -> bool:
        client_id = getattr(dto, 'client_id', 'default_client')
        
        self.eof_count_by_client[client_id] += 1
        logger.info(f"EOF {self.eof_count_by_client[client_id]}/{self.expected_eof_per_client} "
                   f"para cliente {client_id} (routing: {routing_key})")
        
        if self.eof_count_by_client[client_id] >= self.expected_eof_per_client:
            logger.info(f"EOF completo para cliente {client_id} - calculando Top 3 y shardeando")
            
            top_3_results = self.generate_top3_by_store()
            self.send_sharded_to_join_nodes(top_3_results, client_id)            # Limpiar datos de este cliente
            self.store_user_purchases.clear()
            self.eof_count_by_client[client_id] = 0
            
            logger.info(f"Cliente {client_id} completado y limpiado")
        
        return False
    
    def process_message(self, message: bytes, routing_key: str = None, headers: dict = None) -> bool:
        try:
            if self.shutdown.is_shutting_down():
                return True
            
            dto = TransactionBatchDTO.from_bytes_fast(message)
            
            client_id = 'default_client'
            if headers and 'client_id' in headers:
                client_id = headers['client_id']
                if isinstance(client_id, bytes):
                    client_id = client_id.decode('utf-8')
            dto.client_id = client_id
            
            if dto.batch_type == BatchType.EOF:
                return self.handle_eof(dto, routing_key)
            
            if dto.batch_type == BatchType.RAW_CSV:
                for line in dto.data.split('\n'):
                    if line.strip():
                        self.process_csv_line(line.strip())
            
            return False
            
        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")
            return False
    
    def on_message_callback(self, ch, method, properties, body):
        try:
            if self.shutdown.is_shutting_down():
                ch.stop_consuming()
                return
            
            routing_key = getattr(method, 'routing_key', None)
            headers = properties.headers if hasattr(properties, 'headers') else None
            
            self.process_message(body, routing_key, headers)
            
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


if __name__ == "__main__":
    try:
        aggregator = TopCustomersAggregatorNode()
        aggregator.start()
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error fatal: {e}")
        sys.exit(1)