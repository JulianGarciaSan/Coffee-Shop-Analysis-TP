
import logging
import os
import sys
import time
from typing import Callable, Dict, List
from collections import defaultdict

from common.graceful_shutdown import GracefulShutdown
from rabbitmq.middleware import MessageMiddlewareExchangeManual, MessageMiddlewareQueueManual
from dtos.dto import BatchType, MenuItemBatchDTO, StoreBatchDTO, TransactionBatchDTO, TransactionItemBatchDTO, UserBatchDTO
from processors.aggregated_data_processor import AggregatedDataProcessor
from join_engine import JoinEngine
from processors.menu_item_processor import MenuItemProcessor
from processors.store_processor import StoreProcessor
from processors.user_processor import UserProcessor
from processors.processors_handler import ProcessorsHandler
from client_processing_state import ClientProcessingState
from queries.profit_and_selling_query_handler import ProfitAndSellingQueryHandler
from queries.top_customers_query_handler import TopCustomersQueryHandler
from queries.tpv_query_handler import TPVQueryHandler
from checkpoint_handler.checkpoint_handler import CheckpointHandler
from join_node_checkpoint_handler import JoinNodeCheckpointHandler
from healthchecker.healthchecker import HealthChecker


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class JoinNode:    
    def __init__(self):
        self.shutdown = GracefulShutdown()
        self.shutdown.register_callback(self._on_shutdown_signal)
        
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.input_exchange = os.getenv('INPUT_EXCHANGE', 'join.exchange')
        self.output_exchange = os.getenv('OUTPUT_EXCHANGE', 'report.exchange')
        self.checkpoint_dir = os.getenv('CHECKPOINT_DIR', f'/app/server/logs/join/checkpoints')
        self.extra_id = os.getenv('EXTRA_ID', '0')
        self.node_id = int(os.getenv('JOIN_NODE_ID', '0'))
        self.total_join_nodes = int(os.getenv('TOTAL_JOIN_NODES', '3'))
        self.node_name = f"join_node_{self.node_id}"
        
        self.client_states: Dict[str, ClientProcessingState] = defaultdict(ClientProcessingState)
        self.router: Dict[str, Callable] = {}
        self.q3_joined_data_by_client: Dict[str, List[Dict]] = {}
        self.outgoing_counter_by_client: Dict[str, int] = defaultdict(int)
        self.pending_rollbacks: Dict[str, int] = {} 
        self.join_engine = JoinEngine()
        self.processor_handler = ProcessorsHandler(self)
        
        self.join_checkpoint_handler = JoinNodeCheckpointHandler(join_node=self)
        self.checkpoint_handler = CheckpointHandler(checkpoint_dir=self.checkpoint_dir,strategy=self.join_checkpoint_handler, checkpont_interval=3000, outgoing_counter_by_client=self.outgoing_counter_by_client, extra_id=int(self.extra_id))
        self._setup_input_middleware()
        self._setup_output_middleware()
        
        self.intialize_processors()
        self.intialize_query_handlers()
        
        self._setup_message_routes()  
        self._execute_pending_joins_after_recovery()
        
        health_port = int(os.getenv('HEALTH_PORT', '9999'))
        self.health_server = HealthChecker(port=health_port)
        self.health_server.start()  
            
        logger.info("JoinNode inicializado con soporte multi-cliente")
        logger.info(f"  RabbitMQ Host: {self.rabbitmq_host}")
        logger.info(f"  Input Exchange: {self.input_exchange}")
        logger.info(f"  Output Exchange: {self.output_exchange}")
    
    def intialize_processors(self):
        self.store_processors: Dict[str, StoreProcessor] = {}
        self.user_processors: Dict[str, UserProcessor] = {}
        self.menu_item_processors: Dict[str, MenuItemProcessor] = {}
        self.tpv_processors: Dict[str, AggregatedDataProcessor] = {}
        self.top_customers_processors: Dict[str, AggregatedDataProcessor] = {}
        self.best_selling_processors: Dict[str, AggregatedDataProcessor] = {}
        self.most_profit_processors: Dict[str, AggregatedDataProcessor] = {}
        
    def intialize_query_handlers(self):
        self.tpv_query_handler = TPVQueryHandler(self.output_middleware, self)
        self.top_customers_query_handler = TopCustomersQueryHandler(self.output_middleware, self)
        self.profit_and_selling_query_handler = ProfitAndSellingQueryHandler(self.output_middleware, self)

    def _get_or_create_processors(self, client_id: str):
        if client_id not in self.store_processors:
            self.store_processors[client_id] = StoreProcessor(StoreBatchDTO("", BatchType.RAW_CSV))
        
        if client_id not in self.user_processors:
            self.user_processors[client_id] = UserProcessor(UserBatchDTO("", BatchType.RAW_CSV))
        
        if client_id not in self.menu_item_processors:
            self.menu_item_processors[client_id] = MenuItemProcessor(MenuItemBatchDTO("", BatchType.RAW_CSV))
        
        if client_id not in self.tpv_processors:
            self.tpv_processors[client_id] = AggregatedDataProcessor()
        
        if client_id not in self.top_customers_processors:
            self.top_customers_processors[client_id] = AggregatedDataProcessor()
        
        if client_id not in self.best_selling_processors:
            self.best_selling_processors[client_id] = AggregatedDataProcessor()
        
        if client_id not in self.most_profit_processors:
            self.most_profit_processors[client_id] = AggregatedDataProcessor()
    
    def _setup_message_routes(self):
        routes = {
            'stores.data': self.processor_handler.handle_stores_message,
            'users.data': self.processor_handler.handle_users_message,
            'menu_items.data': self.processor_handler.handle_menu_items_message,
            'menu_items.eof': self.processor_handler.handle_menu_items_message,
            'tpv.data': self.tpv_query_handler.handle_tpv_message,
            'top_customers.data': self.top_customers_query_handler.handle_top_customers_message,
            'q2_best_selling.data': self.profit_and_selling_query_handler.handle_best_selling_message,
            'q2_most_profit.data': self.profit_and_selling_query_handler.handle_most_profit_message,
        }
        
        for routing_key, handler in routes.items():
            self.router[routing_key] = handler
        
        logger.info(f"Registradas {len(routes)} rutas de mensajes")
    
    def _setup_input_middleware(self):        
        base_keys = [
            'stores.data',
            'users.data',
            'menu_items.data',
            'tpv.data',
            'top_customers.data',
            'q2_best_selling.data',
            'q2_most_profit.data'
        ]
        
        routing_keys = [f"{self.node_name}.{key}" for key in base_keys]
        
        
        
        logger.info(f"Configurando {len(routing_keys)} routing keys para {self.node_name}")
        logger.info(f"Ejemplos: {routing_keys[:3]}")
        
        self.input_middleware = MessageMiddlewareQueueManual(
            host=self.rabbitmq_host,
            queue_name=f'join_input_queue_{self.node_name}',  
            exchange_name=self.input_exchange,
            routing_keys=routing_keys
        )

        
        if hasattr(self.input_middleware, 'shutdown'):
            self.input_middleware.shutdown = self.shutdown
        
    def _execute_pending_joins_after_recovery(self):
        logger.info("Verificando consistencia transaccional...")
        
        if self.pending_rollbacks:
            for client_id, reset_value in self.pending_rollbacks.items():
                current = self.outgoing_counter_by_client.get(client_id, 0)
                logger.warning(f"ROLLBACK: Restaurando contador de {current} a {reset_value} para reintentar transacción.")
                self.outgoing_counter_by_client[client_id] = reset_value
            self.pending_rollbacks.clear()
            
    def _setup_output_middleware(self):
        self.output_middleware = MessageMiddlewareExchangeManual(
            host=self.rabbitmq_host,
            #exchange_name=self.output_exchange,
            exchange_name='reports_exchange',
            route_keys=['q3.data',
                        'q4.data',
                        'client.*.q3',
                        'client.*.q4', 
                        'q2_best_selling.data',
                        'q2_most_profit.data']
        )
        
        if hasattr(self.output_middleware, 'shutdown'):
            self.output_middleware.shutdown = self.shutdown


    # def start_batch_transaction(self, client_id: str, query_name: str):
    #     current_counter = self.outgoing_counter_by_client.get(client_id, 0)
        
    #     op = f"{client_id},START_TRANSACTION,{current_counter},{query_name}"
    #     self.checkpoint_handler.save_counter_update(client_id, current_counter, [op])

    # def get_next_id_in_memory(self, client_id: str) -> int:
    #     self.outgoing_counter_by_client[client_id] += 1
    #     return int(self.extra_id) * 1_000_000 + self.outgoing_counter_by_client[client_id]

    # def commit_batch_transaction(self, client_id: str, query_name: str):
    #     current_counter = self.outgoing_counter_by_client.get(client_id, 0)
        
    #     ops = [
    #         f"{client_id},COMMIT_TRANSACTION,{current_counter}",
    #         f"{client_id},SENT,{query_name}"
    #     ]
    #     self.checkpoint_handler.save_counter_update(client_id, current_counter, ops)


    def _check_and_execute_joins(self, client_id: str):
        state = self.client_states[client_id]
        
        # Q3: TPV + Stores
        if state.is_q3_ready():
            logger.info(f"Condiciones listas para JOIN Q3 de cliente '{client_id}'")
            joined_data = self.join_engine.join_tpv_stores(
                self.tpv_processors[client_id].get_data(),
                self.store_processors[client_id].get_data()
            )
            self.q3_joined_data_by_client[client_id] = joined_data
            self.tpv_query_handler.send_q3_results(client_id, joined_data)
            state.q3_results_sent = True
            self.checkpoint_handler.save_message_checkpoint(
                client_id, 
                f"q3_sent_{client_id}",  
                [f"SENT:q3"]  
            )
        # Q4: Top Customers + Stores + Users
        if state.is_q4_ready():
            logger.info(f"Condiciones listas para JOIN Q4 de cliente '{client_id}'")
            joined_data = self.join_engine.join_top_customers(
                self.top_customers_processors[client_id].get_data(),
                self.store_processors[client_id].get_data(),
                self.user_processors[client_id].get_data()
            )
            self.top_customers_query_handler.send_q4_results(client_id, joined_data)
            state.q4_results_sent = True
            self.checkpoint_handler.save_message_checkpoint(
                client_id, 
                f"q4_sent_{client_id}",
                [f"SENT:q4"]
            )
        # Q2 Best Selling
        if state.is_best_selling_ready():
            logger.info(f"Condiciones listas para JOIN Q2 Best Selling de cliente '{client_id}'")
            joined_data = self.join_engine.join_with_menu_items(
                self.best_selling_processors[client_id].get_data(),
                self.menu_item_processors[client_id].get_data(),
                'sellings_qty'
            )
            self.profit_and_selling_query_handler.send_best_selling_results(client_id, joined_data)
            state.best_selling_sent = True
            self.checkpoint_handler.save_message_checkpoint(
                client_id,
                f"best_selling_sent_{client_id}",
                [f"SENT:best_selling"]
            )

        # Q2 Most Profit
        if state.is_most_profit_ready():
            logger.info(f"Condiciones listas para JOIN Q2 Most Profit de cliente '{client_id}'")
            joined_data = self.join_engine.join_with_menu_items(
                self.most_profit_processors[client_id].get_data(),
                self.menu_item_processors[client_id].get_data(),
                'profit_sum'
            )
            self.profit_and_selling_query_handler.send_most_profit_results(client_id, joined_data)
            state.most_profit_sent = True
            self.checkpoint_handler.save_message_checkpoint(
                client_id,
                f"most_profit_sent_{client_id}",
                [f"SENT:most_profit"]
            )
            
    def process_message(self, message: bytes, routing_key: str, client_id, message_id) -> bool:
        try:
            handler = self.router.get(routing_key)
            if handler:
                return handler(message, client_id, message_id)
            else:
                logger.warning(f"No handler para routing key: {routing_key}")
                return False
        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}", exc_info=True)
            return False
    
    def _on_shutdown_signal(self):
        logger.info("Señal de shutdown recibida en JoinNode")
        if self.input_middleware:
            self.input_middleware.stop_consuming()
            
            
    def parse_message_headers(self, properties):
        client_id = None
        message_id = None
        if properties and properties.headers:
            client_id = properties.headers.get('client_id')
            message_id = properties.headers.get('message_id')
        return str(client_id), str(message_id)
    
    def parse_routing_key(self, method) -> str:
        routing_key = method.routing_key                

        if '.' in routing_key:
            parts = routing_key.split('.', 1) 
            base_routing_key = parts[1] if len(parts) > 1 else routing_key
        else:
            base_routing_key = routing_key
        return base_routing_key
    
    def on_message_callback(self, ch, method, properties, body):
        try:
            if self.shutdown.is_shutting_down():
                logger.warning("Shutdown solicitado, deteniendo")
                ch.stop_consuming()
                return
            
            base_routing_key = self.parse_routing_key(method)
                
            client_id, message_id = self.parse_message_headers(properties)
            
            if self.checkpoint_handler.analyze_first_message(client_id, message_id, ch, method, body):
                return
            
            should_stop, should_ack = self.process_message(body, base_routing_key, client_id, message_id)

            if should_ack:
                ch.basic_ack(delivery_tag=method.delivery_tag)
            if should_stop:
                logger.info("Procesamiento completado - deteniendo consuming")
                ch.stop_consuming()
                
        except Exception as e:
            logger.error(f"Error en callback: {e}", exc_info=True)
    
    def start(self):
        try:
            self.input_middleware.start_consuming(self.on_message_callback)
            
        except KeyboardInterrupt:
            logger.info("JoinNode detenido manualmente")
        except Exception as e:
            logger.error(f"Error durante el consumo: {e}", exc_info=True)
            raise
        finally:
            self._cleanup()
    
    def _cleanup(self):
        try:
            if hasattr(self, 'input_middleware') and self.input_middleware:
                self.input_middleware.close()
                
            if hasattr(self, 'output_middleware') and self.output_middleware:
                self.output_middleware.close()
                
            logger.info("Conexiones cerradas exitosamente")
            
        except Exception as e:
            logger.error(f"Error durante cleanup: {e}", exc_info=True)
            
    def clean_client_data(self, client_id: str):
        logger.info(f"Limpiando estado para cliente {client_id} en JoinNode")
        try:
            # Remove client state
            if client_id in self.client_states:
                del self.client_states[client_id]
                logger.info(f"Estado de cliente {client_id} limpiado en JoinNode")

            # Remove all processors for the client
            processor_attrs = [
                "store_processors",
                "user_processors",
                "menu_item_processors",
                "tpv_processors",
                "top_customers_processors",
                "best_selling_processors",
                "most_profit_processors",
            ]
            for attr in processor_attrs:
                proc_dict = getattr(self, attr, None)
                if proc_dict and client_id in proc_dict:
                    del proc_dict[client_id]
                    logger.info(f"Procesador '{attr}' eliminado para cliente {client_id}")

            # Clean joined data and counters
            if client_id in self.q3_joined_data_by_client:
                del self.q3_joined_data_by_client[client_id]
                logger.info(f"q3_joined_data eliminado para cliente {client_id}")

            if client_id in self.outgoing_counter_by_client:
                del self.outgoing_counter_by_client[client_id]
                logger.info(f"outgoing_counter eliminado para cliente {client_id}")

            if client_id in self.pending_rollbacks:
                del self.pending_rollbacks[client_id]
                logger.info(f"pending_rollbacks eliminado para cliente {client_id}")
        except Exception as e:
            logger.error(f"Error limpiando estado de cliente {client_id}: {e}", exc_info=True)
                
    def clean_all_data(self):
        """Limpia el estado de TODOS los clientes en JoinNode"""
        logger.info("Limpiando estado de TODOS los clientes en JoinNode")
        
        try:
            self.client_states.clear()
            logger.info("Todos los estados de clientes limpiados")
        except Exception as e:
            logger.error(f"Error limpiando client_states: {e}")

        # Limpiar todos los procesadores
        processor_attrs = [
            "store_processors",
            "user_processors",
            "menu_item_processors",
            "tpv_processors",
            "top_customers_processors",
            "best_selling_processors",
            "most_profit_processors",
        ]
        
        for attr in processor_attrs:
            try:
                proc_dict = getattr(self, attr, None)
                if proc_dict:
                    proc_dict.clear()
                    logger.info(f"Todos los '{attr}' eliminados")
            except Exception as e:
                logger.error(f"Error limpiando '{attr}': {e}")

        # Limpiar joined data y counters
        try:
            self.q3_joined_data_by_client.clear()
            logger.info("Todos los q3_joined_data eliminados")
        except Exception as e:
            logger.error(f"Error limpiando q3_joined_data_by_client: {e}")

        try:
            self.outgoing_counter_by_client.clear()
            logger.info("Todos los outgoing_counter eliminados")
        except Exception as e:
            logger.error(f"Error limpiando outgoing_counter_by_client: {e}")

        try:
            self.pending_rollbacks.clear()
            logger.info("Todos los pending_rollbacks eliminados")
        except Exception as e:
            logger.error(f"Error limpiando pending_rollbacks: {e}")

        logger.info("Limpieza global completada en JoinNode - todos los clientes eliminados")


if __name__ == "__main__":
    try:
        node = JoinNode()
        node.start()
        sys.exit(0)
        
    except Exception as e:
        sys.exit(1)
