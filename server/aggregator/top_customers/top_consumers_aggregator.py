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
from top_customers.utils import TopCustomersAggregatorUtils

from enum import Enum

class ClientState(Enum):
    RECEIVING_DATA = "receiving_data"      
    DATA_SENT = "data_sent"                

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
        
        self.client_states: Dict[str, ClientState] = {}
        self.store_user_purchases_by_client: Dict[str, Dict[str, Dict[str, int]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(int))
        )
        self.outgoing_counter_by_client: Dict[str, int] = defaultdict(int)
        self.pending_rollbacks: Dict[str, int] = {}
        health_port = int(os.getenv('HEALTH_PORT', '9999'))
        self.health_server = HealthChecker(port=health_port)
        self.health_server.start()
        self.top_customers_utils = TopCustomersAggregatorUtils(self)
        self.checkpoint_handler = CheckpointHandler(
            checkpoint_dir=self.checkpoint_dir,
            strategy=self.top_customers_utils,
            checkpont_interval=1,
            outgoing_counter_by_client=self.outgoing_counter_by_client,
            extra_id=int(self.node_id)
        )
        
        self._setup_input_middleware()
        self._setup_output_middleware()
        self.checkpoint_handler.recover_from_checkpoint() 
        self._execute_pending_rollbacks()
        
        
        if hasattr(self.input_middleware, 'shutdown'):
            self.input_middleware.shutdown = self.shutdown
        if hasattr(self.output_middleware, 'shutdown'):
            self.output_middleware.shutdown = self.shutdown
        
        logger.info(f"TopCustomersAggregatorNode {self.node_id} inicializado")
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
        
    def _execute_pending_rollbacks(self):
        if self.pending_rollbacks:
            for client_id, reset_value in self.pending_rollbacks.items():
                current = self.outgoing_counter_by_client.get(client_id, 0)
                logger.warning(f"ROLLBACK: Cliente {client_id} reset contador {current} -> {reset_value}")
                self.outgoing_counter_by_client[client_id] = reset_value
            self.pending_rollbacks.clear()
    
    def _on_shutdown_signal(self):
        logger.info("Señal de shutdown recibida")
        if self.input_middleware:
            self.input_middleware.stop_consuming()

    
    def send_sharded_to_join_nodes(self, top_3_by_store: Dict[str, list], client_id: str):
        batches_by_node = {i: [] for i in range(self.total_join_nodes)}
        
        for store_id, top_users in top_3_by_store.items():
            for user_id, purchases_qty in top_users:
                clean_id = user_id.rstrip('.0') if user_id.endswith('.0') else user_id
                shard_id = int(clean_id) % self.total_join_nodes
                batches_by_node[shard_id].append(f"{store_id},{user_id},{purchases_qty}")
        
        for node_id, batch in batches_by_node.items():
            if not batch: continue
            
            csv_data = 'store_id,user_id,purchases_qty\n' + '\n'.join(batch)
            unique_id = self.checkpoint_handler.get_next_id_in_memory(client_id)
            
            self.output_middleware.send(
                TransactionBatchDTO(csv_data, BatchType.RAW_CSV).to_bytes_fast(),
                routing_key=f"join_node_{node_id}.top_customers.data",
                headers={'client_id': client_id, 'message_id': unique_id}
            )

        for node_id in range(self.total_join_nodes):
            unique_id = self.checkpoint_handler.get_next_id_in_memory(client_id)
            self.output_middleware.send(
                TransactionBatchDTO(f"EOF:{client_id}", BatchType.EOF).to_bytes_fast(),
                routing_key=f"join_node_{node_id}.top_customers.data",
                headers={'client_id': client_id, 'message_id': unique_id}
            )

    def handle_eof(self, dto: TransactionBatchDTO, client_id: str, message_id: str) -> bool:
        current_state = self.client_states.get(client_id, self.ClientState.RECEIVING_DATA)
        
        if current_state == self.ClientState.DATA_SENT:
            logger.info(f"Datos ya enviados para {client_id}. Ignorando.")
            if client_id in self.store_user_purchases_by_client: del self.store_user_purchases_by_client[client_id]
            return False
        
        logger.info(f"Procesando EOF final para {client_id}")
        
        try:
            self.checkpoint_handler.start_batch_transaction(client_id, "top_customers_final")
            
            top_3_results = self.top_customers_utils.generate_top3_by_store(client_id)
            
            self.send_sharded_to_join_nodes(top_3_results, client_id)
            
            self.checkpoint_handler.commit_batch_transaction(client_id, "top_customers_final")

            self.client_states[client_id] = self.ClientState.DATA_SENT
            
            if client_id in self.store_user_purchases_by_client:
                del self.store_user_purchases_by_client[client_id]
            
            return True 
            
        except Exception as e:
            logger.error(f"Error crítico en handle_eof: {e}")
            raise

    def process_message(self, message: bytes, client_id: str = None, message_id: str = None) -> bool:
        try:
            if self.shutdown.is_shutting_down(): return True, False
            dto = TransactionBatchDTO.from_bytes_fast(message)
            
            if dto.batch_type == BatchType.EOF:
                success = self.handle_eof(dto, client_id, message_id)
                if success:
                    self.checkpoint_handler.save_eof_checkpoint(client_id, message_id)
                return False, True 
            
            if dto.batch_type == BatchType.RAW_CSV:
                lines = [l.strip() for l in dto.data.split('\n') if l.strip()]
                for l in lines: self.top_customers_utils.process_csv_line(l, client_id)
                self.checkpoint_handler.save_message_checkpoint(client_id, message_id, lines)
                return False, True
        except: return False, False
        
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
        
            

if __name__ == "__main__":
    try:
        aggregator = TopCustomersAggregatorNode()
        aggregator.start()
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error fatal: {e}")
        sys.exit(1)