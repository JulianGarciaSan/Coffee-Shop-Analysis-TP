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

from enum import Enum

class ClientState(Enum):
    RECEIVING_DATA = "receiving_data"      # Recibiendo datos
    DATA_SENT = "data_sent"                # Datos enviados (puede tener duplicados)
    EOF_COMPLETED = "eof_completed"        # EOF procesado completamente

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

        self.ClientState = ClientState
        self.outgoing_counter_by_client: Dict[str, int] = defaultdict(int)
        
        self.client_states: Dict[str, ClientState] = {}
        # total_stores = 10
        # stores_per_node = total_stores // self.total_nodes
        # extra_stores = total_stores % self.total_nodes
        
        # self.client_logger = LoggerMonitor('/app/client_logs.txt')
        
        # self.message_id = 0
        
        # self.is_first_message = True
        
        # try:
        #     node_num = int(self.node_id)
        # except ValueError:
        #     node_num = int(str(self.node_id).split('_')[-1])
        
        # start_store = (node_num - 1) * stores_per_node + min(node_num - 1, extra_stores)
        # end_store = start_store + stores_per_node + (1 if node_num <= extra_stores else 0)
        
        # self.expected_eof_per_client = end_store - start_store
        
        self.store_user_purchases_by_client: Dict[str, Dict[str, Dict[str, int]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(int))
        )
        # self.eof_count_by_client: Dict[str, int] = defaultdict(int)
        
        health_port = int(os.getenv('HEALTH_PORT', '9999'))
        self.health_server = HealthChecker(port=health_port)
        self.health_server.start()
        
        self.checkpoint_handler = CheckpointHandler(
            checkpoint_dir=self.checkpoint_dir,
            strategy=self,
            checkpont_interval=1
        )
        
        #self._setup_input_middleware(start_store, end_store)
        self._setup_input_middleware()
        self._setup_output_middleware()
        
        if hasattr(self.input_middleware, 'shutdown'):
            self.input_middleware.shutdown = self.shutdown
        if hasattr(self.output_middleware, 'shutdown'):
            self.output_middleware.shutdown = self.shutdown
        
        logger.info(f"TopCustomersAggregatorNode {self.node_id} inicializado")
        # logger.info(f"  Espera {self.expected_eof_per_client} EOF por cliente")
        logger.info(f"  Enviará a {self.total_join_nodes} join nodes")
    
    def _setup_input_middleware(self):
        """Cada aggregator escucha su propia routing key."""
        input_exchange = os.getenv('INPUT_EXCHANGE', 'aggregated.exchange')
        routing_key = f'top_customers_aggregator_{self.node_id}'
        queue_name = os.getenv('INPUT_QUEUE', f'top_customers_aggregator_{self.node_id}')
        
        self.input_middleware = MessageMiddlewareQueueManual(
            host=self.rabbitmq_host,
            queue_name=queue_name,
            exchange_name=input_exchange,
            routing_keys=[routing_key]
        )
        
        logger.info(f"  Input: {input_exchange}")
        logger.info(f"  Routing key: {routing_key}")
    
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
    
    def generate_next_message_id(self, client_id: str) -> int:
        """
        Genera ID único por mensaje para este cliente.
        """
        self.outgoing_counter_by_client[client_id] += 1
        return int(self.node_id) * 1000000 + self.outgoing_counter_by_client[client_id]

    
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
    
    def send_sharded_to_join_nodes(self, top_35_by_store: Dict[str, list], client_id: str, original_message_id: str):
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
            self.send_data_to_join_node(csv_data, client_id, node_id, original_message_id)
                    
        for node_id in range(self.total_join_nodes):
            self.send_eof_to_join_node(client_id, node_id, original_message_id)
        
        logger.info(f"EOF enviado a {self.total_join_nodes} join nodes para cliente {client_id}")

    def send_data_to_join_node(self, csv_data: str, client_id: str, node_id: int, original_message_id: str):
        unique_data_id = self.generate_next_message_id(original_message_id)
        result_dto = TransactionBatchDTO(csv_data, BatchType.RAW_CSV)
        routing_key = f"join_node_{node_id}.top_customers.data"
        
        self.output_middleware.send(
            result_dto.to_bytes_fast(),
            routing_key=routing_key,
            headers={'client_id': client_id, 'message_id': unique_data_id}
        )
    def send_eof_to_join_node(self, client_id: str, node_id: int, original_message_id: str):
        # print("///////////// ENVIANDO EOF A JOIN NODE /////////////")
        # time.sleep(10)
        unique_data_id = self.generate_next_message_id(original_message_id)
        eof_dto = TransactionBatchDTO(f"EOF:{client_id}", BatchType.EOF)
        routing_key = f"join_node_{node_id}.top_customers.data"
        
        self.output_middleware.send(
            eof_dto.to_bytes_fast(),
            routing_key=routing_key,
            headers={'client_id': client_id, 'message_id': unique_data_id}
        )
        
    def handle_eof(self, dto: TransactionBatchDTO, client_id: str, message_id: str) -> bool:
            """
            Maneja EOF con estado del cliente.
            """
            
            current_state = self.client_states.get(client_id, self.ClientState.RECEIVING_DATA)
            
            logger.info(f"EOF recibido para cliente {client_id}, estado: {current_state.value}")
 
            if current_state == self.ClientState.DATA_SENT:
                logger.info(f"Datos ya enviados para {client_id} - ignorando EOF duplicado")
                
                if client_id in self.store_user_purchases_by_client:
                    del self.store_user_purchases_by_client[client_id]
                    logger.info(f"Memoria liberada para cliente {client_id}")
                
                return False
            
            logger.info(f"Procesando EOF para cliente {client_id}")
            
            try:
                top_3_results = self.generate_top3_by_store(client_id)
                logger.info(f"   Top 3 calculado: {len(top_3_results)} stores")
                
                self.send_sharded_to_join_nodes(top_3_results, client_id,message_id)
                logger.info(f"   Datos enviados a join nodes")
                
                self.client_states[client_id] = self.ClientState.DATA_SENT
                
                
                if client_id in self.store_user_purchases_by_client:
                    del self.store_user_purchases_by_client[client_id]
                
                logger.info(f"Cliente {client_id} completado y limpiado")
                
            except Exception as e:
                logger.error(f"Error procesando EOF para {client_id}: {e}")
                raise
            
            return False

    def process_message(self, message: bytes, client_id: str = None, message_id: str = None) -> bool:
        try:
            if self.shutdown.is_shutting_down():
                return (True, False)
            
            dto = TransactionBatchDTO.from_bytes_fast(message)
                    
            if dto.batch_type == BatchType.EOF:
                self.handle_eof(dto, client_id, message_id)
                self.checkpoint_handler.save_eof_checkpoint(client_id, message_id)
                return (False, True) 
            
            if dto.batch_type == BatchType.RAW_CSV:
                lines = [line.strip() for line in dto.data.split('\n') if line.strip()]
                
                for i, line in enumerate(lines):
                    try:
                        self.process_csv_line(line, client_id)
                    except Exception as e:
                        logger.warning(f"Línea {i} inválida: {e}")
                
                self.checkpoint_handler.save_message_checkpoint(
                    client_id=client_id,
                    message_id=message_id,
                    csv_lines=lines
                )
                
                return (False, True)
            
        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")
            return (False, False)
        
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
            
            # self.checkpoint_handler.register_incoming_message(client_id, message_id)
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
        Serializa estado completo incluyendo contador de mensajes salientes.
        """
        client_data = self.store_user_purchases_by_client.get(client_id, {})
        
        serialized = {
            "stores": {},
            "client_state": self.client_states.get(
                client_id, 
                self.ClientState.RECEIVING_DATA
            ).value,
            "outgoing_counter": self.outgoing_counter_by_client.get(client_id, 0)  # ← NUEVO
        }
        
        for store_id, users in client_data.items():
            serialized["stores"][store_id] = {}
            for user_id, count in users.items():
                serialized["stores"][store_id][user_id] = count
        
        return serialized

    def _deserialize_client_data(self, client_id: str, data: Dict):
        """
        Reconstruye estado incluyendo contador de mensajes.
        """
        self.store_user_purchases_by_client[client_id] = defaultdict(lambda: defaultdict(int))
        
        stores_data = data.get("stores", {})
        for store_id, users in stores_data.items():
            for user_id, count in users.items():
                self.store_user_purchases_by_client[client_id][store_id][user_id] = count
        
        # Restaurar estado del cliente
        state_str = data.get("client_state", "receiving_data")
        try:
            self.client_states[client_id] = self.ClientState(state_str)
            logger.info(f"   Estado restaurado: {client_id} → {state_str}")
        except ValueError:
            self.client_states[client_id] = self.ClientState.RECEIVING_DATA
        
        # Restaurar contador de mensajes salientes
        self.outgoing_counter_by_client[client_id] = data.get("outgoing_counter", 0)
        logger.info(f"   Contador saliente restaurado: {self.outgoing_counter_by_client[client_id]}")

            
            

if __name__ == "__main__":
    try:
        aggregator = TopCustomersAggregatorNode()
        aggregator.start()
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error fatal: {e}")
        sys.exit(1)