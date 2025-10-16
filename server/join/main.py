import gc
import json
import logging
import os
import sys
from typing import Dict, List
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
import threading

from common.graceful_shutdown import GracefulShutdown
from rabbitmq.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
from dtos.dto import BatchType, MenuItemBatchDTO, StoreBatchDTO, TransactionBatchDTO, TransactionItemBatchDTO, UserBatchDTO
from aggregated_data_processor import AggregatedDataProcessor
from join_engine import JoinEngine
from message_router import MessageRouter
from menu_item_processor import MenuItemProcessor
from store_processor import StoreProcessor
from user_processor import UserProcessor
from client_routing.client_routing import ClientRouter
from peer_dto import PeerUserRequest, PeerUserResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataSourceType(Enum):
    STORES = "stores"
    USERS = "users"
    MENU_ITEMS = "menu_items"
    TPV = "tpv"
    TOP_CUSTOMERS = "top_customers"
    BEST_SELLING = "q2_best_selling"
    MOST_PROFIT = "q2_most_profit"

@dataclass
class PeerCleanupSignal:
    """Señal para coordinar limpieza entre nodos."""
    sending_node_id: int
    client_id: str
    
    def to_bytes(self) -> bytes:
        data = {
            'sending_node_id': self.sending_node_id,
            'client_id': self.client_id
        }
        return json.dumps(data).encode('utf-8')
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'PeerCleanupSignal':
        obj = json.loads(data.decode('utf-8'))
        return cls(
            sending_node_id=obj['sending_node_id'],
            client_id=obj['client_id']
        )
        
@dataclass
class ClientProcessingState:
    stores_loaded: bool = False
    users_loaded: bool = False
    menu_items_loaded: bool = False
    top_customers_eof_count: int = 0
    best_selling_loaded: bool = False
    most_profit_loaded: bool = False
    groupby_eof_count: int = 0
    expected_groupby_nodes: int = 2
    expected_top_customers_aggregators: int = 2
    
    q3_results_sent: bool = False
    q4_results_sent: bool = False
    best_selling_sent: bool = False
    most_profit_sent: bool = False
    
    # Para peer-to-peer communication
    pending_top_customers: List[Dict] = field(default_factory=list)
    requested_users: set = field(default_factory=set)
    received_peer_users: Dict[str, Dict] = field(default_factory=dict)
    peer_requests_pending: int = 0
    top_customers_processed: bool = False 
    
    nodes_ready_to_cleanup: set = field(default_factory=set)
    cleanup_broadcasted: bool = False
    
    def is_q3_ready(self) -> bool:
        return (self.stores_loaded and 
                self.groupby_eof_count >= self.expected_groupby_nodes and 
                not self.q3_results_sent)
    
    def is_q4_ready(self) -> bool:
        return (self.stores_loaded and 
                self.users_loaded and 
                self.top_customers_eof_count >= self.expected_top_customers_aggregators and
                self.top_customers_processed and  
                self.peer_requests_pending == 0 and
                not self.q4_results_sent)
    
    def is_best_selling_ready(self) -> bool:
        return (self.menu_items_loaded and 
                self.best_selling_loaded and 
                not self.best_selling_sent)
    
    def is_most_profit_ready(self) -> bool:
        return (self.menu_items_loaded and 
                self.most_profit_loaded and 
                not self.most_profit_sent)

    def all_queries_completed(self) -> bool:
        return (self.q3_results_sent and 
                self.q4_results_sent and 
                self.best_selling_sent and 
                self.most_profit_sent)

class JoinNode:    
    def __init__(self):
        self.shutdown = GracefulShutdown()
        self.shutdown.register_callback(self._on_shutdown_signal)
        
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.input_exchange = os.getenv('INPUT_EXCHANGE', 'join.exchange')
        self.output_exchange = os.getenv('OUTPUT_EXCHANGE', 'report.exchange')
        self.peer_exchange = os.getenv('PEER_EXCHANGE', 'join.peer.exchange')
        
        self.node_id = int(os.getenv('JOIN_NODE_ID', '0'))
        self.total_join_nodes = int(os.getenv('TOTAL_JOIN_NODES', '3'))
        self.total_topk_aggregators = int(os.getenv('TOTAL_TOPK_NODES', '2'))
        self.total_groupby_semester_nodes = int(os.getenv('TOTAL_GROUPBY_SEMESTER_NODES', '2'))
        self.node_name = f"join_node_{self.node_id}"
        self.cleanup_required_nodes = self.total_join_nodes


        self.client_states: Dict[str, ClientProcessingState] = defaultdict(
            lambda: ClientProcessingState(
                expected_groupby_nodes=self.total_groupby_semester_nodes,
                expected_top_customers_aggregators=self.total_topk_aggregators
            )
        )
        
        self.peer_lock = threading.Lock()
        
        self.store_processors: Dict[str, StoreProcessor] = {}
        self.user_processors: Dict[str, UserProcessor] = {}
        self.menu_item_processors: Dict[str, MenuItemProcessor] = {}
        self.tpv_processors: Dict[str, AggregatedDataProcessor] = {}
        self.top_customers_processors: Dict[str, AggregatedDataProcessor] = {}
        self.best_selling_processors: Dict[str, AggregatedDataProcessor] = {}
        self.most_profit_processors: Dict[str, AggregatedDataProcessor] = {}
        
        self.q3_joined_data_by_client: Dict[str, List[Dict]] = {}
        
        self.join_engine = JoinEngine()
        self.client_locks: Dict[str, threading.Lock] = defaultdict(threading.Lock)

        self.router = MessageRouter()
        self._setup_message_routes()
        
        self._setup_middleware()
        self._setup_peer_middleware()
        
        logger.info("JoinNode inicializado con peer-to-peer communication")
        logger.info(f"  Node ID: {self.node_id}")
        logger.info(f"  Total Join Nodes: {self.total_join_nodes}")
        logger.info(f"  RabbitMQ Host: {self.rabbitmq_host}")
        logger.info(f"  Input Exchange: {self.input_exchange}")
        logger.info(f"  Output Exchange: {self.output_exchange}")
        logger.info(f"  Peer Exchange: {self.peer_exchange}")
        
        self.client_router = ClientRouter(
            total_join_nodes=self.total_join_nodes,
            node_prefix="join_node"
        )
    
    def _get_or_create_processors(self, client_id: str):
        if client_id not in self.store_processors:
            self.store_processors[client_id] = StoreProcessor(StoreBatchDTO("", BatchType.RAW_CSV))
            logger.info(f"StoreProcessor creado para cliente '{client_id}'")
        
        if client_id not in self.user_processors:
            self.user_processors[client_id] = UserProcessor(UserBatchDTO("", BatchType.RAW_CSV))
            logger.info(f"UserProcessor creado para cliente '{client_id}'")
        
        if client_id not in self.menu_item_processors:
            self.menu_item_processors[client_id] = MenuItemProcessor(MenuItemBatchDTO("", BatchType.RAW_CSV))
            logger.info(f"MenuItemProcessor creado para cliente '{client_id}'")
        
        if client_id not in self.tpv_processors:
            self.tpv_processors[client_id] = AggregatedDataProcessor()
            logger.info(f"TPV Processor creado para cliente '{client_id}'")
        
        if client_id not in self.top_customers_processors:
            self.top_customers_processors[client_id] = AggregatedDataProcessor()
            logger.info(f"TopCustomers Processor creado para cliente '{client_id}'")
        
        if client_id not in self.best_selling_processors:
            self.best_selling_processors[client_id] = AggregatedDataProcessor()
            logger.info(f"BestSelling Processor creado para cliente '{client_id}'")
        
        if client_id not in self.most_profit_processors:
            self.most_profit_processors[client_id] = AggregatedDataProcessor()
            logger.info(f"MostProfit Processor creado para cliente '{client_id}'")
    
    def _setup_message_routes(self):
        routes = {
            'stores.data': self._handle_stores_message,
            'users.data': self._handle_users_message,
            'menu_items.data': self._handle_menu_items_message,
            'menu_items.eof': self._handle_menu_items_message,
            'tpv.data': self._handle_tpv_message,
            'top_customers.data': self._handle_top_customers_message,
            'top_customers.eof': self._handle_top_customers_message,
            'q2_best_selling.data': self._handle_best_selling_message,
            'q2_most_profit.data': self._handle_most_profit_message,
            
            # Peer communication
            'user_request': self._handle_peer_user_request,
            'user_response': self._handle_peer_user_response,
            'cleanup_signal': self._handle_peer_cleanup_signal,
        }
        
        for routing_key, handler in routes.items():
            self.router.register(routing_key, handler)
        
        logger.info(f"Registradas {len(routes)} rutas de mensajes")
    
    def _setup_middleware(self):        
        base_keys = [
            'stores.data',
            'users.data',
            'menu_items.data',
            'tpv.data',
            'top_customers.data', 'top_customers.eof',
            'q2_best_selling.data',
            'q2_most_profit.data'
        ]
        
        routing_keys = [f"{self.node_name}.{key}" for key in base_keys]
        
        logger.info(f"Configurando {len(routing_keys)} routing keys para {self.node_name}")
        
        self.input_middleware = MessageMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=f'join_input_queue_{self.node_name}',
            exchange_name=self.input_exchange,
            routing_keys=routing_keys
        )
        
        if hasattr(self.input_middleware, 'shutdown'):
            self.input_middleware.shutdown = self.shutdown
        
        self.output_middleware = MessageMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name=self.output_exchange,
            route_keys=['q3.data', 'q3.eof', 'q4.data', 'q4.eof', 
                       'q2_best_selling.data', 'q2_best_selling.eof',
                       'q2_most_profit.data', 'q2_most_profit.eof']
        )
        
        if hasattr(self.output_middleware, 'shutdown'):
            self.output_middleware.shutdown = self.shutdown
    
    def _setup_peer_middleware(self):        
        self.peer_input_middleware = MessageMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=f'join_node_{self.node_id}_peer_queue',
            exchange_name=self.peer_exchange,
            routing_keys=[
                f'join_node_{self.node_id}.user_request',
                f'join_node_{self.node_id}.user_response',
                f'join_node_{self.node_id}.cleanup_signal',
            ]
        )
        route_keys = []
        for i in range(self.total_join_nodes):
            route_keys.append(f'join_node_{i}.user_request')
            route_keys.append(f'join_node_{i}.user_response')
            route_keys.append(f'join_node_{i}.cleanup_signal') 
        
        self.peer_output_middleware_main = MessageMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name=self.peer_exchange,
            route_keys=route_keys
        )
        
        self.peer_output_middleware_peer = MessageMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name=self.peer_exchange,
            route_keys=route_keys
        )
        
        if hasattr(self.peer_input_middleware, 'shutdown'):
            self.peer_input_middleware.shutdown = self.shutdown
        if hasattr(self.peer_output_middleware_main, 'shutdown'):
            self.peer_output_middleware_main.shutdown = self.shutdown
        if hasattr(self.peer_output_middleware_peer, 'shutdown'):
            self.peer_output_middleware_peer.shutdown = self.shutdown
    
    def _extract_client_id(self, headers: dict) -> str:
        client_id = 'default_client'
        if headers and 'client_id' in headers:
            original_value = headers['client_id']
            client_id = original_value
            if isinstance(client_id, bytes):
                client_id = client_id.decode('utf-8')
            final_client_id = str(client_id)
            return final_client_id
        return client_id
    
    def _handle_stores_message(self, message: bytes, headers: dict = None) -> bool:
        client_id = self._extract_client_id(headers)
        self._get_or_create_processors(client_id)
        
        dto = StoreBatchDTO.from_bytes_fast(message)
        
        if dto.batch_type == BatchType.EOF:
            self.client_states[client_id].stores_loaded = True
            stores_count = len(self.store_processors[client_id].get_data())
            logger.info(f"EOF recibido de stores para cliente '{client_id}': {stores_count} stores cargados")
            self._check_and_execute_joins(client_id)
            return False
        
        if dto.batch_type == BatchType.RAW_CSV:
            self.store_processors[client_id].process_batch(dto.data)
            logger.debug(f"Batch de stores procesado para cliente '{client_id}'")
        return False
    
    def _handle_users_message(self, message: bytes, headers: dict = None) -> bool:
        client_id = self._extract_client_id(headers)
        self._get_or_create_processors(client_id)
        
        dto = UserBatchDTO.from_bytes_fast(message)
        
        if dto.batch_type == BatchType.EOF:
            self.client_states[client_id].users_loaded = True
            users_count = len(self.user_processors[client_id].get_data())
            logger.info(f"EOF recibido de users para cliente '{client_id}': {users_count} users cargados")
            
            self._check_and_execute_joins(client_id)
            return False
        
        if dto.batch_type == BatchType.RAW_CSV:
            self.user_processors[client_id].process_batch(dto.data)
            logger.debug(f"Batch de users procesado para cliente '{client_id}'")
        return False
    
    def _handle_menu_items_message(self, message: bytes, headers: dict = None) -> bool:
        client_id = self._extract_client_id(headers)
        self._get_or_create_processors(client_id)
        
        dto = MenuItemBatchDTO.from_bytes_fast(message)
        
        if dto.batch_type == BatchType.EOF:
            self.client_states[client_id].menu_items_loaded = True
            menu_items_count = len(self.menu_item_processors[client_id].get_data())
            logger.info(f"EOF recibido de menu_items para cliente '{client_id}': {menu_items_count} items cargados")
            self._check_and_execute_joins(client_id)
            return False
        
        if dto.batch_type == BatchType.RAW_CSV:
            self.menu_item_processors[client_id].process_batch(dto.data)
            logger.debug(f"Batch de menu_items procesado para cliente '{client_id}'")
        return False
    
    def _handle_tpv_message(self, message: bytes, headers: dict = None) -> bool:
        client_id = self._extract_client_id(headers)
        self._get_or_create_processors(client_id)
        
        dto = TransactionBatchDTO.from_bytes_fast(message)
        
        if dto.batch_type == BatchType.EOF:
            self.client_states[client_id].groupby_eof_count += 1
            logger.info(f"EOF TPV recibido para cliente '{client_id}': {self.client_states[client_id].groupby_eof_count}/{self.client_states[client_id].expected_groupby_nodes}")
            self._check_and_execute_joins(client_id)
            return False
        
        if dto.batch_type == BatchType.RAW_CSV:
            self.tpv_processors[client_id].process_batch(dto.data, self._parse_tpv_line)
        return False
    
    def _handle_top_customers_message(self, message: bytes, headers: dict = None) -> bool:
        client_id = self._extract_client_id(headers)
        self._get_or_create_processors(client_id)
        
        dto = TransactionBatchDTO.from_bytes_fast(message)
        
        if dto.batch_type == BatchType.EOF:
            self.client_states[client_id].top_customers_eof_count += 1
            
            logger.info(
                f"EOF top_customers para cliente '{client_id}': "
                f"{self.client_states[client_id].top_customers_eof_count}/"
                f"{self.client_states[client_id].expected_top_customers_aggregators}"
            )
            
            if self.client_states[client_id].top_customers_eof_count >= \
            self.client_states[client_id].expected_top_customers_aggregators:
                logger.info(f"✓ Todos los EOF de top_customers recibidos para cliente '{client_id}'")
                
                self._check_and_execute_joins(client_id)
            
            return False
        
        if dto.batch_type == BatchType.RAW_CSV:
            lines = dto.data.split('\n')
            
            for line in lines:
                if not line.strip() or line.startswith('store_id'):
                    continue
                
                parsed = self._parse_top_customers_line(line)
                if parsed:
                    self.client_states[client_id].pending_top_customers.append(parsed)
            
            logger.debug(f"Top customers acumulados para cliente '{client_id}': "
                        f"{len(self.client_states[client_id].pending_top_customers)}")
        
        return False
    
    def _process_pending_top_customers(self, client_id: str):
        
        state = self.client_states[client_id]
        users_dict = self.user_processors[client_id].get_data()
        
        missing_users_by_node = {i: set() for i in range(self.total_join_nodes)}
        
        logger.info(f"Procesando {len(state.pending_top_customers)} top_customers para cliente '{client_id}'")
        
        for top_customer in state.pending_top_customers:
            user_id = top_customer['user_id']
            
            if user_id in users_dict:
                self.top_customers_processors[client_id].get_data().append(top_customer)
                logger.debug(f"✓ user_id={user_id} disponible localmente")
            
            elif user_id in state.received_peer_users:
                self.top_customers_processors[client_id].get_data().append(top_customer)
                logger.debug(f"✓ user_id={user_id} recibido de peer")
            
            else:
                normalized_user_id = user_id.split('.')[0] if '.' in user_id else user_id
                target_node = hash(normalized_user_id) % self.total_join_nodes
                
                if target_node == self.node_id:
                    logger.warning(f"⚠ user_id={user_id} debería estar aquí pero falta")
                else:
                    if user_id not in state.requested_users:
                        missing_users_by_node[target_node].add(user_id)
                        state.requested_users.add(user_id)
                        logger.debug(f"→ user_id={user_id} será solicitado a join_node_{target_node}")
        
        with self.peer_lock:
            for target_node, user_ids in missing_users_by_node.items():
                if user_ids and target_node != self.node_id:
                    self._request_users_from_peer(target_node, client_id, list(user_ids))
                    state.peer_requests_pending += 1

        
        logger.info(f"Cliente '{client_id}': {state.peer_requests_pending} peer requests enviadas, "
                   f"{len(self.top_customers_processors[client_id].get_data())} top_customers procesados inmediatamente")
    
    def _request_users_from_peer(self, target_node_id: int, client_id: str, user_ids: list):
        
        request = PeerUserRequest(
            requesting_node_id=self.node_id,
            client_id=client_id,
            user_ids=user_ids
        )
        
        routing_key = f'join_node_{target_node_id}.user_request'
        
        self.peer_output_middleware_main.send(
            request.to_bytes(),
            routing_key=routing_key,
            headers={'client_id': client_id}
        )
        
        logger.info(f"Solicitud enviada a join_node_{target_node_id}: {len(user_ids)} users para cliente '{client_id}'")

    def _handle_peer_user_request(self, message: bytes, headers: dict = None) -> bool:
        
        request = PeerUserRequest.from_bytes(message)
        client_id = request.client_id
        requesting_node = request.requesting_node_id
        
        logger.info(f"Recibida solicitud de join_node_{requesting_node} para cliente '{client_id}': "
                f"{len(request.user_ids)} users")
        
        self._get_or_create_processors(client_id)
        users_dict = self.user_processors[client_id].get_data()
        
        found_users = {}
        missing_users = []
        
        for user_id in request.user_ids:
            if user_id in users_dict:
                found_users[user_id] = users_dict[user_id]
            else:
                missing_users.append(user_id)
        
        if missing_users:
            logger.warning(f"No se encontraron {len(missing_users)} users (primeros 5): {missing_users[:5]}")
        
        response = PeerUserResponse(
            responding_node_id=self.node_id,
            client_id=client_id,
            users_data=found_users
        )
        
        routing_key = f'join_node_{requesting_node}.user_response'
        
        self.peer_output_middleware_peer.send(
            response.to_bytes(),
            routing_key=routing_key,
            headers={'client_id': client_id}
        )
        
        logger.info(f"Respuesta enviada a join_node_{requesting_node}: {len(found_users)}/{len(request.user_ids)} users encontrados")
        
        return False
    
    
    def _handle_peer_user_response(self, message: bytes, headers: dict = None) -> bool:
        
        response = PeerUserResponse.from_bytes(message)
        client_id = response.client_id
        responding_node = response.responding_node_id
        
        logger.info(f"Recibida respuesta de join_node_{responding_node} para cliente '{client_id}': "
                   f"{len(response.users_data)} users")
        
        state = self.client_states[client_id]
        
        state.received_peer_users.update(response.users_data)
        
        with self.peer_lock:
            state.peer_requests_pending -= 1
        
        logger.info(f"Cliente '{client_id}': requests pendientes = {state.peer_requests_pending}")
        
        self._process_top_customers_with_new_users(client_id, response.users_data.keys())
        
        self._check_and_execute_joins(client_id)
        
        return False
    
    def _handle_peer_cleanup_signal(self, message: bytes, headers: dict = None) -> bool:
        """Maneja señales de cleanup."""
        try:
            signal = PeerCleanupSignal.from_bytes(message)
            client_id = signal.client_id
            
            logger.info(f"Recibida señal de cleanup para cliente '{client_id}'")
            
            # Limpiar inmediatamente
            self._cleanup_client_data(client_id)
            
            return False
            
        except Exception as e:
            logger.error(f"Error manejando cleanup signal: {e}", exc_info=True)
            return False
    
    def _process_top_customers_with_new_users(self, client_id: str, new_user_ids: set):
        
        state = self.client_states[client_id]
        users_dict = self.user_processors[client_id].get_data()
        
        all_users = {**users_dict, **state.received_peer_users}
        
        processed_count = 0
        
        for top_customer in state.pending_top_customers:
            user_id = top_customer['user_id']
            
            if user_id in new_user_ids and user_id in all_users:
                if top_customer not in self.top_customers_processors[client_id].get_data():
                    self.top_customers_processors[client_id].get_data().append(top_customer)
                    processed_count += 1
        
        logger.info(f"Procesados {processed_count} top_customers adicionales con nuevos users")
    
    def _handle_best_selling_message(self, message: bytes, headers: dict = None) -> bool:
        client_id = self._extract_client_id(headers)
        self._get_or_create_processors(client_id)
        
        dto = TransactionItemBatchDTO.from_bytes_fast(message)
        
        if dto.batch_type == BatchType.EOF:
            self.client_states[client_id].best_selling_loaded = True
            logger.info(f"EOF recibido de best_selling para cliente '{client_id}'")
            self._check_and_execute_joins(client_id)
            return False
        
        if dto.batch_type == BatchType.RAW_CSV:
            self.best_selling_processors[client_id].process_batch(dto.data, self._parse_best_selling_line)
        return False
    
    def _handle_most_profit_message(self, message: bytes, headers: dict = None) -> bool:
        client_id = self._extract_client_id(headers)
        self._get_or_create_processors(client_id)
        
        dto = TransactionItemBatchDTO.from_bytes_fast(message)
        
        if dto.batch_type == BatchType.EOF:
            self.client_states[client_id].most_profit_loaded = True
            logger.info(f"EOF recibido de most_profit para cliente '{client_id}'")
            self._check_and_execute_joins(client_id)
            return False
        
        if dto.batch_type == BatchType.RAW_CSV:
            self.most_profit_processors[client_id].process_batch(dto.data, self._parse_most_profit_line)
        return False
    
    def _parse_tpv_line(self, line: str) -> Dict:
        if line.startswith('year_half_created_at'):
            return None
        parts = line.split(',')
        if len(parts) >= 4:
            return {
                'year_half_created_at': parts[0],
                'store_id': parts[1],
                'total_payment_value': float(parts[2]),
                'transaction_count': int(parts[3])
            }
        return None
    
    def _parse_top_customers_line(self, line: str) -> Dict:
        if line.startswith('store_id,user_id,purchases_qty'):
            return None
        parts = line.split(',')
        if len(parts) >= 3:
            raw_user_id = parts[1]
            user_id = raw_user_id.split('.')[0] if '.' in raw_user_id else raw_user_id
            return {
                'store_id': parts[0],
                'user_id': user_id,
                'purchases_qty': int(parts[2])
            }
        return None
    
    def _parse_best_selling_line(self, line: str) -> Dict:
        if line.startswith('created_at'):
            return None
        parts = line.split(',')
        if len(parts) >= 3:
            return {
                'year_month_created_at': parts[0],
                'item_id': parts[1],
                'sellings_qty': int(parts[2])
            }
        return None
    
    def _parse_most_profit_line(self, line: str) -> Dict:
        if line.startswith('created_at'):
            return None
        parts = line.split(',')
        if len(parts) >= 3:
            return {
                'year_month_created_at': parts[0],
                'item_id': parts[1],
                'profit_sum': float(parts[2])
            }
        return None
    
    def _check_and_execute_joins(self, client_id: str):
        with self.client_locks[client_id]:
            state = self.client_states[client_id]
        
            if (not state.top_customers_processed and 
                state.users_loaded and 
                state.top_customers_eof_count >= state.expected_top_customers_aggregators):
                
                if state.pending_top_customers:
                    logger.info(f"Users disponibles, procesando {len(state.pending_top_customers)} top_customers para cliente '{client_id}'")
                    self._process_pending_top_customers(client_id)
                else:
                    logger.info(f"No hay top_customers para cliente '{client_id}' en este nodo (sharding) - marcando como procesado")
                
                state.top_customers_processed = True
            
            logger.debug(f"[Q4 CHECK] Cliente '{client_id}':")
            logger.debug(f"  stores_loaded: {state.stores_loaded}")
            logger.debug(f"  users_loaded: {state.users_loaded}")
            logger.debug(f"  top_customers_eof_count: {state.top_customers_eof_count}/{state.expected_top_customers_aggregators}")
            logger.debug(f"  top_customers_processed: {state.top_customers_processed}")
            logger.debug(f"  peer_requests_pending: {state.peer_requests_pending}")
            logger.debug(f"  q4_results_sent: {state.q4_results_sent}")
            
            # Q3: TPV + Stores
            if state.is_q3_ready():
                logger.info(f"Condiciones listas para JOIN Q3 de cliente '{client_id}'")
                joined_data = self.join_engine.join_tpv_stores(
                    self.tpv_processors[client_id].get_data(),
                    self.store_processors[client_id].get_data()
                )
                self.q3_joined_data_by_client[client_id] = joined_data
                self._send_q3_results(client_id, joined_data)
                state.q3_results_sent = True
            
            # Q4: Top Customers + Stores + Users (con peer users)
            if state.is_q4_ready():
                logger.info(f" Condiciones listas para JOIN Q4 de cliente '{client_id}'")
                logger.info(f"  Top customers procesados: {len(self.top_customers_processors[client_id].get_data())}")
                logger.info(f"  Users locales: {len(self.user_processors[client_id].get_data())}")
                logger.info(f"  Users de peers: {len(state.received_peer_users)}")
                
                joined_data = self.join_engine.join_top_customers(
                    self.top_customers_processors[client_id].get_data(),
                    self.store_processors[client_id].get_data(),
                    self.user_processors[client_id].get_data(),
                    state.received_peer_users
                )
                self._send_q4_results(client_id, joined_data)
                state.q4_results_sent = True
            else:
                if not state.q4_results_sent:
                    reasons = []
                    if not state.stores_loaded:
                        reasons.append("stores not loaded")
                    if not state.users_loaded:
                        reasons.append("users not loaded")
                    if state.top_customers_eof_count < state.expected_top_customers_aggregators:
                        reasons.append(f"EOF pending ({state.top_customers_eof_count}/{state.expected_top_customers_aggregators})")
                    if not state.top_customers_processed:
                        reasons.append("top_customers not processed yet")
                    if state.peer_requests_pending > 0:
                        reasons.append(f"peer requests pending ({state.peer_requests_pending})")
                    
                    logger.debug(f"Q4 no listo para cliente '{client_id}': {', '.join(reasons)}")
            
            # Q2 Best Selling 
            if state.is_best_selling_ready():
                logger.info(f"Condiciones listas para JOIN Q2 Best Selling de cliente '{client_id}'")
                joined_data = self.join_engine.join_with_menu_items(
                    self.best_selling_processors[client_id].get_data(),
                    self.menu_item_processors[client_id].get_data(),
                    'sellings_qty'
                )
                self._send_best_selling_results(client_id, joined_data)
                state.best_selling_sent = True
            
            # Q2 Most Profit 
            if state.is_most_profit_ready():
                logger.info(f"Condiciones listas para JOIN Q2 Most Profit de cliente '{client_id}'")
                joined_data = self.join_engine.join_with_menu_items(
                    self.most_profit_processors[client_id].get_data(),
                    self.menu_item_processors[client_id].get_data(),
                    'profit_sum'
                )
                self._send_most_profit_results(client_id, joined_data)
                state.most_profit_sent = True
                
            if state.all_queries_completed():
                if not state.cleanup_broadcasted:
                    self._broadcast_cleanup_signal(client_id)
                    state.cleanup_broadcasted = True
                    state.nodes_ready_to_cleanup.add(self.node_id)
    
    def _send_q3_results(self, client_id: str, joined_data: List[Dict]):
        try:
            if not joined_data:
                logger.warning(f"No hay datos para Q3 de cliente '{client_id}'")
                return
            
            sorted_data = sorted(joined_data, 
                               key=lambda x: (x['year_half_created_at'], int(x['store_id'])))
            
            BATCH_SIZE = 150
            header = "year_half_created_at,store_name,tpv"
            
            logger.info(f"Enviando Q3 para cliente '{client_id}': {len(sorted_data)} registros")
            
            for i in range(0, len(sorted_data), BATCH_SIZE):
                batch = sorted_data[i:i + BATCH_SIZE]
                csv_lines = [header]
                
                for record in batch:
                    store_name = str(record['store_name']).replace(',', '_')
                    csv_lines.append(f"{record['year_half_created_at']},{store_name},{record['tpv']:.1f}")
                
                results_csv = '\n'.join(csv_lines)
                
                result_dto = TransactionBatchDTO(results_csv, BatchType.RAW_CSV)
                self.output_middleware.send(
                    result_dto.to_bytes_fast(), 
                    routing_key='q3.data',
                    headers={'client_id': int(client_id)}
                )
                
                logger.info(f"Batch Q3 enviado para cliente '{client_id}': {len(batch)} registros")
            
            eof_dto = TransactionBatchDTO(f"EOF:{client_id}", BatchType.EOF)
            self.output_middleware.send(
                eof_dto.to_bytes_fast(), 
                routing_key='q3.data',
                headers={'client_id': int(client_id)}
            )
            
            logger.info(f"Resultados Q3 completados para cliente '{client_id}'")
            
        except Exception as e:
            logger.error(f"Error enviando resultados Q3 para cliente '{client_id}': {e}", exc_info=True)
    
    def _send_q4_results(self, client_id: str, joined_data: List[Dict]):
        try:
            if not joined_data:
                logger.warning(f"No hay datos para Q4 de cliente '{client_id}'")
                return
            
            sorted_data = sorted(joined_data, key=lambda x: int(x['store_id']))
            
            BATCH_SIZE = 1000
            header = "store_name,birthdate"
            
            for i in range(0, len(sorted_data), BATCH_SIZE):
                batch = sorted_data[i:i + BATCH_SIZE]
                csv_lines = [header]
                
                for record in batch:
                    csv_lines.append(f"{record['store_name']},{record['birthdate']}")
                
                results_csv = '\n'.join(csv_lines)
                
                result_dto = TransactionBatchDTO(results_csv, BatchType.RAW_CSV)
                self.output_middleware.send(
                    result_dto.to_bytes_fast(), 
                    routing_key='q4.data',
                    headers={'client_id': int(client_id)}
                )
                
                logger.info(f"Batch Q4 enviado para cliente '{client_id}': {len(batch)} registros")
            
            eof_dto = TransactionBatchDTO(f"EOF:{client_id}", BatchType.EOF)
            self.output_middleware.send(
                eof_dto.to_bytes_fast(), 
                routing_key='q4.data',
                headers={'client_id': int(client_id)}
            )
            
            logger.info(f"Resultados Q4 completados para cliente '{client_id}': {len(joined_data)} registros")
            
        except Exception as e:
            logger.error(f"Error enviando resultados Q4 para cliente '{client_id}': {e}", exc_info=True)
    
    def _send_best_selling_results(self, client_id: str, joined_data: List[Dict]):
        try:
            if not joined_data:
                logger.warning(f"No hay datos para Q2 best_selling de cliente '{client_id}'")
                return
            
            sorted_data = sorted(joined_data, key=lambda x: x['year_month_created_at'])
            
            BATCH_SIZE = 1000
            header = "year_month_created_at,item_name,sellings_qty"
            
            for i in range(0, len(sorted_data), BATCH_SIZE):
                batch = sorted_data[i:i + BATCH_SIZE]
                csv_lines = [header]
                
                for record in batch:
                    csv_lines.append(f"{record['year_month_created_at']},{record['item_name']},{record['sellings_qty']}")
                
                results_csv = '\n'.join(csv_lines)
                
                result_dto = TransactionItemBatchDTO(results_csv, BatchType.RAW_CSV)
                self.output_middleware.send(
                    result_dto.to_bytes_fast(), 
                    routing_key='q2_best_selling.data',
                    headers={'client_id': int(client_id)}
                )
                
                logger.info(f"Batch Q2 best_selling enviado para cliente '{client_id}': {len(batch)} registros")
            
            eof_dto = TransactionItemBatchDTO(f"EOF:{client_id}", BatchType.EOF)
            self.output_middleware.send(
                eof_dto.to_bytes_fast(), 
                routing_key='q2_best_selling.data',
                headers={'client_id': int(client_id)}
            )
            
            logger.info(f"Resultados Q2 best_selling completados para cliente '{client_id}'")
            
        except Exception as e:
            logger.error(f"Error enviando resultados Q2 best_selling para cliente '{client_id}': {e}", exc_info=True)
    
    def _send_most_profit_results(self, client_id: str, joined_data: List[Dict]):
        try:
            if not joined_data:
                logger.warning(f"No hay datos para Q2 most_profit de cliente '{client_id}'")
                return
            
            sorted_data = sorted(joined_data, key=lambda x: x['year_month_created_at'])
            
            BATCH_SIZE = 1000
            header = "year_month_created_at,item_name,profit_sum"
            
            for i in range(0, len(sorted_data), BATCH_SIZE):
                batch = sorted_data[i:i + BATCH_SIZE]
                csv_lines = [header]
                
                for record in batch:
                    csv_lines.append(f"{record['year_month_created_at']},{record['item_name']},{record['profit_sum']:.1f}")
                
                results_csv = '\n'.join(csv_lines)
                
                result_dto = TransactionItemBatchDTO(results_csv, BatchType.RAW_CSV)
                self.output_middleware.send(
                    result_dto.to_bytes_fast(), 
                    routing_key='q2_most_profit.data',
                    headers={'client_id': int(client_id)}
                )
                
                logger.info(f"Batch Q2 most_profit enviado para cliente '{client_id}': {len(batch)} registros")
            
            eof_dto = TransactionItemBatchDTO(f"EOF:{client_id}", BatchType.EOF)
            self.output_middleware.send(
                eof_dto.to_bytes_fast(), 
                routing_key='q2_most_profit.data',
                headers={'client_id': int(client_id)}
            )
            
            logger.info(f"Resultados Q2 most_profit completados para cliente '{client_id}'")
            
        except Exception as e:
            logger.error(f"Error enviando resultados Q2 most_profit para cliente '{client_id}': {e}", exc_info=True)
    
    def process_message(self, message: bytes, routing_key: str, headers: dict = None) -> bool:
        if self.shutdown.is_shutting_down():
            logger.warning("Shutdown en progreso, ignorando mensaje")
            return True
        
        try:
            return self.router.route(routing_key, message, headers)
        except Exception as e:
            logger.error(f"Error procesando mensaje con routing key {routing_key}: {e}", exc_info=True)
            return False
    
    def _on_shutdown_signal(self):
        logger.info("Señal de shutdown recibida en JoinNode")
        if self.input_middleware:
            self.input_middleware.stop_consuming()
        if self.peer_input_middleware:
            self.peer_input_middleware.stop_consuming()
    
    def on_message_callback(self, ch, method, properties, body):
        try:
            if self.shutdown.is_shutting_down():
                logger.warning("Shutdown solicitado, deteniendo")
                ch.stop_consuming()
                return
            
            routing_key = method.routing_key
            headers = properties.headers if properties and hasattr(properties, 'headers') else None
            
            if '.' in routing_key:
                parts = routing_key.split('.', 1)
                base_routing_key = parts[1] if len(parts) > 1 else routing_key
            else:
                base_routing_key = routing_key
            
            should_stop = self.process_message(body, base_routing_key, headers)

            if should_stop:
                logger.info("Procesamiento completado - deteniendo consuming")
                ch.stop_consuming()
                
        except Exception as e:
            logger.error(f"Error en callback: {e}", exc_info=True)
    
    def on_peer_message_callback(self, ch, method, properties, body):
        try:
            if self.shutdown.is_shutting_down():
                ch.stop_consuming()
                return
            
            routing_key = method.routing_key
            headers = properties.headers if properties and hasattr(properties, 'headers') else None
            
            parts = routing_key.split('.', 1)
            message_type = parts[1] if len(parts) > 1 else routing_key
            
            self.process_message(body, message_type, headers)
            
        except Exception as e:
            logger.error(f"Error en peer callback: {e}", exc_info=True)
    
    def start(self):
        try:
            main_thread = threading.Thread(
                target=self._consume_main_exchange,
                daemon=False
            )
            
            peer_thread = threading.Thread(
                target=self._consume_peer_exchange,
                daemon=False
            )
            
            main_thread.start()
            peer_thread.start()
            
            logger.info("JoinNode consumiendo de main y peer exchanges")
            
            main_thread.join()
            peer_thread.join()
            
        except KeyboardInterrupt:
            logger.info("JoinNode detenido manualmente")
        except Exception as e:
            logger.error(f"Error durante el consumo: {e}", exc_info=True)
            raise
        finally:
            self._cleanup()
    
    def _consume_main_exchange(self):
        try:
            logger.info("Iniciando consumo de main exchange")
            self.input_middleware.start_consuming(self.on_message_callback)
        except Exception as e:
            logger.error(f"Error en main exchange: {e}")
    
    def _consume_peer_exchange(self):
        try:
            logger.info("Iniciando consumo de peer exchange")
            self.peer_input_middleware.start_consuming(self.on_peer_message_callback)
        except Exception as e:
            logger.error(f"Error en peer exchange: {e}")
    
    def _cleanup(self):
        try:
            if hasattr(self, 'input_middleware') and self.input_middleware:
                self.input_middleware.close()
                
            if hasattr(self, 'peer_input_middleware') and self.peer_input_middleware:
                self.peer_input_middleware.close()
                
            if hasattr(self, 'output_middleware') and self.output_middleware:
                self.output_middleware.close()
                
            if hasattr(self, 'peer_output_middleware_main') and self.peer_output_middleware_main:
                self.peer_output_middleware_main.close()
                
            if hasattr(self, 'peer_output_middleware_peer') and self.peer_output_middleware_peer:
                self.peer_output_middleware_peer.close()
                
            logger.info("Conexiones cerradas exitosamente")
            
        except Exception as e:
            logger.error(f"Error durante cleanup: {e}", exc_info=True)

    def _cleanup_client_data(self, client_id: str):
        """
        Limpia todos los datos acumulados de un cliente específico.
        Se llama después de enviar todos los EOFs.
        """
        try:
            logger.info(f"Iniciando limpieza de datos para cliente '{client_id}'")
            
            if client_id in self.store_processors:
                self.store_processors[client_id].get_data().clear()
                del self.store_processors[client_id]
                logger.debug(f"Store processor eliminado")
            
            if client_id in self.user_processors:
                self.user_processors[client_id].get_data().clear()
                del self.user_processors[client_id]
                logger.debug(f"User processor eliminado")
            
            if client_id in self.menu_item_processors:
                self.menu_item_processors[client_id].get_data().clear()
                del self.menu_item_processors[client_id]
                logger.debug(f"Menu item processor eliminado")
            
            if client_id in self.tpv_processors:
                self.tpv_processors[client_id].get_data().clear()
                del self.tpv_processors[client_id]
                logger.debug(f"TPV processor eliminado")
            
            if client_id in self.top_customers_processors:
                self.top_customers_processors[client_id].get_data().clear()
                del self.top_customers_processors[client_id]
                logger.debug(f"Top customers processor eliminado")
            
            if client_id in self.best_selling_processors:
                self.best_selling_processors[client_id].get_data().clear()
                del self.best_selling_processors[client_id]
                logger.debug(f"Best selling processor eliminado")
            
            if client_id in self.most_profit_processors:
                self.most_profit_processors[client_id].get_data().clear()
                del self.most_profit_processors[client_id]
                logger.debug(f"Most profit processor eliminado")
            
            # Limpiar datos de joins
            if client_id in self.q3_joined_data_by_client:
                del self.q3_joined_data_by_client[client_id]
                logger.debug(f"Q3 joined data eliminado")
            
            # Limpiar estado del cliente
            if client_id in self.client_states:
                state = self.client_states[client_id]
                state.pending_top_customers.clear()
                state.requested_users.clear()
                state.received_peer_users.clear()
                del self.client_states[client_id]
                logger.debug(f"Client state eliminado")
                    
            if client_id in self.client_locks:
                del self.client_locks[client_id]
                logger.debug(f"Client lock eliminado")
            
            collected = gc.collect()
            
            logger.info(f"Limpieza completada para cliente '{client_id}'")
            
        except Exception as e:
            logger.error(f"Error limpiando datos del cliente '{client_id}': {e}", exc_info=True)
    
    def _broadcast_cleanup_signal(self, client_id: str):
        """Broadcast a todos los nodos que pueden limpiar este cliente."""
        try:
            signal = PeerCleanupSignal(
                sending_node_id=self.node_id,  
                client_id=client_id
            )            
            for target_node in range(self.total_join_nodes):
                routing_key = f'join_node_{target_node}.cleanup_signal'
                
                with self.peer_lock:
                    self.peer_output_middleware_main.send(
                        signal.to_bytes(),
                        routing_key=routing_key,
                        headers={'client_id': client_id}
                    )
            
            logger.info(f"✓ Cleanup signal enviado a {self.total_join_nodes} nodos para cliente '{client_id}'")
            
        except Exception as e:
            logger.error(f"Error broadcasting cleanup signal: {e}", exc_info=True)
    

if __name__ == "__main__":
    
    try:
        node = JoinNode()
        node.start()
        sys.exit(0)
        
    except Exception as e:
        sys.exit(1)
