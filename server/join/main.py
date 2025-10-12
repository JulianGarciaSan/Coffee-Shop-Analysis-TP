import logging
import os
import sys
from typing import Dict, List
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

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
class ClientProcessingState:
    stores_loaded: bool = False
    users_loaded: bool = False
    menu_items_loaded: bool = False
    top_customers_loaded: bool = False
    best_selling_loaded: bool = False
    most_profit_loaded: bool = False
    groupby_eof_count: int = 0
    expected_groupby_nodes: int = 2
    
    q3_results_sent: bool = False
    q4_results_sent: bool = False
    best_selling_sent: bool = False
    most_profit_sent: bool = False
    
    def is_q3_ready(self) -> bool:
        return (self.stores_loaded and 
                self.groupby_eof_count >= self.expected_groupby_nodes and 
                not self.q3_results_sent)
    
    def is_q4_ready(self) -> bool:
        return (self.stores_loaded and 
                self.users_loaded and 
                self.top_customers_loaded and
                not self.q4_results_sent)
    
    def is_best_selling_ready(self) -> bool:
        return (self.menu_items_loaded and 
                self.best_selling_loaded and 
                not self.best_selling_sent)
    
    def is_most_profit_ready(self) -> bool:
        return (self.menu_items_loaded and 
                self.most_profit_loaded and 
                not self.most_profit_sent)


class JoinNode:    
    def __init__(self):
        self.shutdown = GracefulShutdown()
        self.shutdown.register_callback(self._on_shutdown_signal)
        
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.input_exchange = os.getenv('INPUT_EXCHANGE', 'join.exchange')
        self.output_exchange = os.getenv('OUTPUT_EXCHANGE', 'report.exchange')
        
        self.node_id = int(os.getenv('JOIN_NODE_ID', '0'))
        self.total_join_nodes = int(os.getenv('TOTAL_JOIN_NODES', '3'))
        self.node_name = f"join_node_{self.node_id}"
        
        self.client_states: Dict[str, ClientProcessingState] = defaultdict(ClientProcessingState)
        
        self.store_processors: Dict[str, StoreProcessor] = {}
        self.user_processors: Dict[str, UserProcessor] = {}
        self.menu_item_processors: Dict[str, MenuItemProcessor] = {}
        self.tpv_processors: Dict[str, AggregatedDataProcessor] = {}
        self.top_customers_processors: Dict[str, AggregatedDataProcessor] = {}
        self.best_selling_processors: Dict[str, AggregatedDataProcessor] = {}
        self.most_profit_processors: Dict[str, AggregatedDataProcessor] = {}
        
        self.q3_joined_data_by_client: Dict[str, List[Dict]] = {}
        
        self.join_engine = JoinEngine()
        
        self.router = MessageRouter()
        self._setup_message_routes()
        
        self._setup_middleware()
        
        logger.info("JoinNode inicializado con soporte multi-cliente")
        logger.info(f"  RabbitMQ Host: {self.rabbitmq_host}")
        logger.info(f"  Input Exchange: {self.input_exchange}")
        logger.info(f"  Output Exchange: {self.output_exchange}")
        
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
            'q2_best_selling.eof': self._handle_best_selling_message,
            'q2_most_profit.data': self._handle_most_profit_message,
            'q2_most_profit.eof': self._handle_most_profit_message
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
            'q2_most_profit.data', 'q2_most_profit.eof'
        ]
        
        routing_keys = [f"{self.node_name}.{key}" for key in base_keys]
        
        logger.info(f"Configurando {len(routing_keys)} routing keys para {self.node_name}")
        logger.info(f"Ejemplos: {routing_keys[:3]}")
        
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
    
    def _extract_client_id(self, headers: dict) -> str:
        client_id = 'default_client'
        if headers and 'client_id' in headers:
            client_id = headers['client_id']
            if isinstance(client_id, bytes):
                client_id = client_id.decode('utf-8')
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
            self.client_states[client_id].top_customers_loaded = True
            logger.info(f"EOF recibido de top_customers para cliente '{client_id}'")
            self._check_and_execute_joins(client_id)
            return False
        
        if dto.batch_type == BatchType.RAW_CSV:
            self.top_customers_processors[client_id].process_batch(dto.data, self._parse_top_customers_line)
        return False
    
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
            user_id = raw_user_id[:-2] if '.' in raw_user_id and raw_user_id.endswith('.0') else raw_user_id
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
        """Verifica y ejecuta joins para un cliente específico"""
        state = self.client_states[client_id]
        
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
        
        # Q4: Top Customers + Stores + Users
        if state.is_q4_ready():
            logger.info(f"Condiciones listas para JOIN Q4 de cliente '{client_id}'")
            joined_data = self.join_engine.join_top_customers(
                self.top_customers_processors[client_id].get_data(),
                self.store_processors[client_id].get_data(),
                self.user_processors[client_id].get_data()
            )
            self._send_q4_results(client_id, joined_data)
            state.q4_results_sent = True
        
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
    
    def _send_q3_results(self, client_id: str, joined_data: List[Dict]):
        try:
            if not joined_data:
                logger.warning(f"No hay datos joinados para Q3 de cliente '{client_id}'")
                return
            
            sorted_data = sorted(joined_data, 
                               key=lambda x: (x['year_half_created_at'], int(x['store_id'])))
            
            csv_lines = ["year_half_created_at,store_name,tpv"]
            for record in sorted_data:
                csv_lines.append(f"{record['year_half_created_at']},{record['store_name']},{record['tpv']:.1f}")
            
            results_csv = '\n'.join(csv_lines)
            
            result_dto = TransactionBatchDTO(results_csv, BatchType.RAW_CSV)
            self.output_middleware.send(
                result_dto.to_bytes_fast(), 
                routing_key='q3.data',
                headers={'client_id': client_id}
            )
            
            eof_dto = TransactionBatchDTO(f"EOF:{client_id}", BatchType.EOF)
            self.output_middleware.send(
                eof_dto.to_bytes_fast(), 
                routing_key='q3.eof',
                headers={'client_id': client_id}
            )
            
            logger.info(f"Resultados Q3 enviados para cliente '{client_id}': {len(joined_data)} registros")
            
        except Exception as e:
            logger.error(f"Error enviando resultados Q3 para cliente '{client_id}': {e}", exc_info=True)
    
    def _send_q4_results(self, client_id: str, joined_data: List[Dict]):
        try:
            if not joined_data:
                logger.warning(f"No hay datos para Q4 de cliente '{client_id}'")
                return
            
            sorted_data = sorted(joined_data, key=lambda x: int(x['store_id']))
            
            csv_lines = ["store_name,birthdate"]
            for record in sorted_data:
                csv_lines.append(f"{record['store_name']},{record['birthdate']}")
            
            results_csv = '\n'.join(csv_lines)
            
            result_dto = TransactionBatchDTO(results_csv, BatchType.RAW_CSV)
            self.output_middleware.send(
                result_dto.to_bytes_fast(), 
                routing_key='q4.data',
                headers={'client_id': client_id}
            )
            
            eof_dto = TransactionBatchDTO(f"EOF:{client_id}", BatchType.EOF)
            self.output_middleware.send(
                eof_dto.to_bytes_fast(), 
                routing_key='q4.eof',
                headers={'client_id': client_id}
            )
            
            logger.info(f"Resultados Q4 enviados para cliente '{client_id}': {len(joined_data)} registros")
            
        except Exception as e:
            logger.error(f"Error enviando resultados Q4 para cliente '{client_id}': {e}", exc_info=True)
    
    def _send_best_selling_results(self, client_id: str, joined_data: List[Dict]):
        try:
            if not joined_data:
                logger.warning(f"No hay datos para Q2 best_selling de cliente '{client_id}'")
                return
            
            sorted_data = sorted(joined_data, key=lambda x: x['year_month_created_at'])
            
            csv_lines = ["year_month_created_at,item_name,sellings_qty"]
            for record in sorted_data:
                csv_lines.append(f"{record['year_month_created_at']},{record['item_name']},{record['sellings_qty']}")
            
            results_csv = '\n'.join(csv_lines)
            
            result_dto = TransactionItemBatchDTO(results_csv, BatchType.RAW_CSV)
            self.output_middleware.send(
                result_dto.to_bytes_fast(), 
                routing_key='q2_best_selling.data',
                headers={'client_id': client_id}
            )
            
            eof_dto = TransactionItemBatchDTO(f"EOF:{client_id}", BatchType.EOF)
            self.output_middleware.send(
                eof_dto.to_bytes_fast(), 
                routing_key='q2_best_selling.eof',
                headers={'client_id': client_id}
            )
            
            logger.info(f"Resultados Q2 best_selling enviados para cliente '{client_id}': {len(joined_data)} registros")
            
        except Exception as e:
            logger.error(f"Error enviando resultados Q2 best_selling para cliente '{client_id}': {e}", exc_info=True)
    
    def _send_most_profit_results(self, client_id: str, joined_data: List[Dict]):
        try:
            if not joined_data:
                logger.warning(f"No hay datos para Q2 most_profit de cliente '{client_id}'")
                return
            
            sorted_data = sorted(joined_data, key=lambda x: x['year_month_created_at'])
            
            csv_lines = ["year_month_created_at,item_name,profit_sum"]
            for record in sorted_data:
                csv_lines.append(f"{record['year_month_created_at']},{record['item_name']},{record['profit_sum']:.1f}")
            
            results_csv = '\n'.join(csv_lines)
            
            result_dto = TransactionItemBatchDTO(results_csv, BatchType.RAW_CSV)
            self.output_middleware.send(
                result_dto.to_bytes_fast(), 
                routing_key='q2_most_profit.data',
                headers={'client_id': client_id}
            )
            
            eof_dto = TransactionItemBatchDTO(f"EOF:{client_id}", BatchType.EOF)
            self.output_middleware.send(
                eof_dto.to_bytes_fast(), 
                routing_key='q2_most_profit.eof',
                headers={'client_id': client_id}
            )
            
            logger.info(f"Resultados Q2 most_profit enviados para cliente '{client_id}': {len(joined_data)} registros")
            
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


if __name__ == "__main__":
    try:
        node = JoinNode()
        node.start()
        sys.exit(0)
        
    except Exception as e:
        sys.exit(1)