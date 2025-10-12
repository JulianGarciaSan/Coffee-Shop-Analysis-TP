import logging
import os
import sys
from typing import Dict, List
from dataclasses import dataclass
from enum import Enum

from common.graceful_shutdown import GracefulShutdown
from rabbitmq.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
from dtos.dto import BatchType, MenuItemBatchDTO, StoreBatchDTO, TransactionBatchDTO, TransactionItemBatchDTO, UserBatchDTO
from aggregated_data_processor import AggregatedDataProcessor
from join_engine import JoinEngine
from message_router import MessageRouter
from menu_item_processor import MenuItemProcessor
from store_processor import StoreProcessor
from user_processor import UserProcessor

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
class ProcessingState:
    stores_loaded: bool = False
    users_loaded: bool = False
    menu_items_loaded: bool = False
    top_customers_loaded: bool = False
    best_selling_loaded: bool = False
    most_profit_loaded: bool = False
    groupby_eof_count: int = 0
    expected_groupby_nodes: int = 2
    
    q3_results_sent: bool = False
    top_customers_sent: bool = False
    best_selling_sent: bool = False
    most_profit_sent: bool = False
    
    def is_q3_ready(self) -> bool:
        return (self.stores_loaded and 
                self.groupby_eof_count >= self.expected_groupby_nodes and 
                not self.q3_results_sent)
    
    def is_q4_ready(self) -> bool:
        return (self.stores_loaded and 
                self.users_loaded and 
                not self.top_customers_sent)
    
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
        self.state = ProcessingState()
        
        self.store_processor = StoreProcessor(StoreBatchDTO("", BatchType.RAW_CSV))
        self.user_processor = UserProcessor(UserBatchDTO("", BatchType.RAW_CSV))
        self.menu_item_processor = MenuItemProcessor(MenuItemBatchDTO("", BatchType.RAW_CSV))
        
        self.tpv_processor = AggregatedDataProcessor()
        self.top_customers_processor = AggregatedDataProcessor()
        self.best_selling_processor = AggregatedDataProcessor()
        self.most_profit_processor = AggregatedDataProcessor()
        
        self.q3_joined_data: List[Dict] = []
        
        self.join_engine = JoinEngine()
        
        self.router = MessageRouter()
        self._setup_message_routes()
        
        self._setup_middleware()
        
        logger.info("JoinNode inicializado con arquitectura refactorizada")
        logger.info(f"  RabbitMQ Host: {self.rabbitmq_host}")
        logger.info(f"  Input Exchange: {self.input_exchange}")
        logger.info(f"  Output Exchange: {self.output_exchange}")
    
    def _setup_message_routes(self):
        routes = {
            'stores.data': self._handle_stores_message,
            'users.data': self._handle_users_message,
            'users.eof': self._handle_users_message,
            'menu_items.data': self._handle_menu_items_message,
            'tpv.data': self._handle_tpv_message,
            'tpv.eof': self._handle_tpv_message,
            'top_customers.data': self._handle_top_customers_message,
            'top_customers.eof': self._handle_top_customers_message,
            'q2_best_selling.data': self._handle_best_selling_message,
            'q2_most_profit.data': self._handle_most_profit_message
        }
        
        for routing_key, handler in routes.items():
            self.router.register(routing_key, handler)
        
        logger.info(f"Registradas {len(routes)} rutas de mensajes")
    
    def _setup_middleware(self):
        self.input_middleware = MessageMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name='join_input_queue',
            exchange_name=self.input_exchange,
            routing_keys=['stores.data', 'tpv.data', 'tpv.eof', 
                       'top_customers.data', 'top_customers.eof',
                       'users.data', 'users.eof',
                       'menu_items.data',
                       'q2_best_selling.data', 'q2_most_profit.data']
        )
        
        if hasattr(self.input_middleware, 'shutdown'):
            self.input_middleware.shutdown = self.shutdown
        
        self.output_middleware = MessageMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name=self.output_exchange,
            route_keys=['q3.data', 'q3.eof', 'q4.data', 'q4.eof', 
                       'q2_best_selling.data', 'q2_most_profit.data']
        )
        
        if hasattr(self.output_middleware, 'shutdown'):
            self.output_middleware.shutdown = self.shutdown
        
    def _handle_stores_message(self, message: bytes) -> bool:
        dto = StoreBatchDTO.from_bytes_fast(message)
        
        if dto.batch_type == BatchType.EOF:
            self.state.stores_loaded = True
            logger.info("EOF recibido de stores")
            self._check_and_execute_joins()
            return False
        
        if dto.batch_type == BatchType.RAW_CSV:
            self.store_processor.process_batch(dto.data)
        return False
    
    def _handle_users_message(self, message: bytes) -> bool:
        dto = UserBatchDTO.from_bytes_fast(message)
        
        if dto.batch_type == BatchType.EOF:
            self.state.users_loaded = True
            logger.info("EOF recibido de users")
            self._check_and_execute_joins()
            return False
        
        if dto.batch_type == BatchType.RAW_CSV:
            self.user_processor.process_batch(dto.data)
        return False
    
    def _handle_menu_items_message(self, message: bytes) -> bool:
        dto = MenuItemBatchDTO.from_bytes_fast(message)
        
        if dto.batch_type == BatchType.EOF:
            self.state.menu_items_loaded = True
            logger.info("EOF recibido de menu_items")
            self._check_and_execute_joins()
            return False
        
        if dto.batch_type == BatchType.RAW_CSV:
            self.menu_item_processor.process_batch(dto.data)
        return False
    
    def _handle_tpv_message(self, message: bytes) -> bool:
        dto = TransactionBatchDTO.from_bytes_fast(message)
        
        if dto.batch_type == BatchType.EOF:
            self.state.groupby_eof_count += 1
            logger.info(f"EOF TPV recibido: {self.state.groupby_eof_count}/{self.state.expected_groupby_nodes}")
            self._check_and_execute_joins()
            return False
        
        if dto.batch_type == BatchType.RAW_CSV:
            self.tpv_processor.process_batch(dto.data, self._parse_tpv_line)
        return False
    
    def _handle_top_customers_message(self, message: bytes) -> bool:
        dto = TransactionBatchDTO.from_bytes_fast(message)
        
        if dto.batch_type == BatchType.EOF:
            self.state.top_customers_loaded = True
            logger.info("EOF recibido de top_customers")
            self._check_and_execute_joins()
            return False
        
        if dto.batch_type == BatchType.RAW_CSV:
            self.top_customers_processor.process_batch(dto.data, self._parse_top_customers_line)
        return False
    
    def _handle_best_selling_message(self, message: bytes) -> bool:
        dto = TransactionItemBatchDTO.from_bytes_fast(message)
        
        if dto.batch_type == BatchType.EOF:
            self.state.best_selling_loaded = True
            logger.info("EOF recibido de best_selling")
            self._check_and_execute_joins()
            return False
        
        if dto.batch_type == BatchType.RAW_CSV:
            self.best_selling_processor.process_batch(dto.data, self._parse_best_selling_line)
        return False
    
    def _handle_most_profit_message(self, message: bytes) -> bool:
        dto = TransactionItemBatchDTO.from_bytes_fast(message)
        
        if dto.batch_type == BatchType.EOF:
            self.state.most_profit_loaded = True
            logger.info("EOF recibido de most_profit")
            self._check_and_execute_joins()
            return False
        
        if dto.batch_type == BatchType.RAW_CSV:
            self.most_profit_processor.process_batch(dto.data, self._parse_most_profit_line)
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
        
    def _check_and_execute_joins(self):        
        # Q3: TPV + Stores
        if self.state.is_q3_ready():
            logger.info("Condiciones listas para JOIN Q3")
            self.q3_joined_data = self.join_engine.join_tpv_stores(
                self.tpv_processor.get_data(),
                self.store_processor.get_data()
            )
            self._send_q3_results()
            self.state.q3_results_sent = True
        
        # Q4: Top Customers + Stores + Users
        if self.state.is_q4_ready():
            logger.info("Condiciones listas para JOIN Q4")
            joined_data = self.join_engine.join_top_customers(
                self.top_customers_processor.get_data(),
                self.store_processor.get_data(),
                self.user_processor.get_data()
            )
            self._send_q4_results(joined_data)
            self.state.top_customers_sent = True
        
        # Q2 Best Selling
        if self.state.is_best_selling_ready():
            logger.info("Condiciones listas para JOIN Q2 Best Selling")
            joined_data = self.join_engine.join_with_menu_items(
                self.best_selling_processor.get_data(),
                self.menu_item_processor.get_data(),
                'sellings_qty'
            )
            self._send_best_selling_results(joined_data)
            self.state.best_selling_sent = True
        
        # Q2 Most Profit
        if self.state.is_most_profit_ready():
            logger.info("Condiciones listas para JOIN Q2 Most Profit")
            joined_data = self.join_engine.join_with_menu_items(
                self.most_profit_processor.get_data(),
                self.menu_item_processor.get_data(),
                'profit_sum'
            )
            self._send_most_profit_results(joined_data)
            self.state.most_profit_sent = True
    
    
    def _send_q3_results(self):
        try:
            if not self.q3_joined_data:
                logger.warning("No hay datos joinados para Q3")
                return
            
            sorted_data = sorted(self.q3_joined_data, 
                               key=lambda x: (x['year_half_created_at'], int(x['store_id'])))
            
            csv_lines = ["year_half_created_at,store_name,tpv"]
            for record in sorted_data:
                csv_lines.append(f"{record['year_half_created_at']},{record['store_name']},{record['tpv']:.1f}")
            
            results_csv = '\n'.join(csv_lines)
            
            result_dto = TransactionBatchDTO(results_csv, BatchType.RAW_CSV)
            self.output_middleware.send(result_dto.to_bytes_fast(), routing_key='q3.data')
            
            eof_dto = TransactionBatchDTO("EOF:1", BatchType.EOF)
            self.output_middleware.send(eof_dto.to_bytes_fast(), routing_key='q3.eof')
            
            logger.info(f"Resultados Q3 enviados: {len(self.q3_joined_data)} registros")
            
        except Exception as e:
            logger.error(f"Error enviando resultados Q3: {e}", exc_info=True)
    
    def _send_q4_results(self, joined_data: List[Dict]):
        try:
            if not joined_data:
                logger.warning("No hay datos para Q4")
                return
            
            sorted_data = sorted(joined_data, key=lambda x: int(x['store_id']))
            
            csv_lines = ["store_name,birthdate"]
            for record in sorted_data:
                csv_lines.append(f"{record['store_name']},{record['birthdate']}")
            
            results_csv = '\n'.join(csv_lines)
            
            result_dto = TransactionBatchDTO(results_csv, BatchType.RAW_CSV)
            self.output_middleware.send(result_dto.to_bytes_fast(), routing_key='q4.data')
            
            eof_dto = TransactionBatchDTO("EOF:1", BatchType.EOF)
            self.output_middleware.send(eof_dto.to_bytes_fast(), routing_key='q4.eof')
            
            logger.info(f"Resultados Q4 enviados: {len(joined_data)} registros")
            
        except Exception as e:
            logger.error(f"Error enviando resultados Q4: {e}", exc_info=True)
    
    def _send_best_selling_results(self, joined_data: List[Dict]):
        try:
            if not joined_data:
                logger.warning("No hay datos para Q2 best_selling")
                return
            
            sorted_data = sorted(joined_data, key=lambda x: x['year_month_created_at'])
            
            csv_lines = ["year_month_created_at,item_name,sellings_qty"]
            for record in sorted_data:
                csv_lines.append(f"{record['year_month_created_at']},{record['item_name']},{record['sellings_qty']}")
            
            results_csv = '\n'.join(csv_lines)
            
            result_dto = TransactionItemBatchDTO(results_csv, BatchType.RAW_CSV)
            self.output_middleware.send(result_dto.to_bytes_fast(), routing_key='q2_best_selling.data')
            
            eof_dto = TransactionItemBatchDTO("EOF:1", BatchType.EOF)
            self.output_middleware.send(eof_dto.to_bytes_fast(), routing_key='q2_best_selling.data')
            
            logger.info(f"Resultados Q2 best_selling enviados: {len(joined_data)} registros")
            
        except Exception as e:
            logger.error(f"Error enviando resultados Q2 best_selling: {e}", exc_info=True)
    
    def _send_most_profit_results(self, joined_data: List[Dict]):
        try:
            if not joined_data:
                logger.warning("No hay datos para Q2 most_profit")
                return
            
            sorted_data = sorted(joined_data, key=lambda x: x['year_month_created_at'])
            
            csv_lines = ["year_month_created_at,item_name,profit_sum"]
            for record in sorted_data:
                csv_lines.append(f"{record['year_month_created_at']},{record['item_name']},{record['profit_sum']:.1f}")
            
            results_csv = '\n'.join(csv_lines)
            
            result_dto = TransactionItemBatchDTO(results_csv, BatchType.RAW_CSV)
            self.output_middleware.send(result_dto.to_bytes_fast(), routing_key='q2_most_profit.data')
            
            eof_dto = TransactionItemBatchDTO("EOF:1", BatchType.EOF)
            self.output_middleware.send(eof_dto.to_bytes_fast(), routing_key='q2_most_profit.data')
            
            logger.info(f"Resultados Q2 most_profit enviados: {len(joined_data)} registros")
            
        except Exception as e:
            logger.error(f"Error enviando resultados Q2 most_profit: {e}", exc_info=True)
    
    
    def process_message(self, message: bytes, routing_key: str) -> bool:
        if self.shutdown.is_shutting_down():
            logger.warning("Shutdown en progreso, ignorando mensaje")
            return True
        
        try:
            return self.router.route(routing_key, message)
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
            should_stop = self.process_message(body, routing_key)
            
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