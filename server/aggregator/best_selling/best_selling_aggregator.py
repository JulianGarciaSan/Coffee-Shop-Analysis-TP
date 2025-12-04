import logging
import os
import sys
import time
from typing import Any, Dict, Optional, Tuple
from collections import defaultdict
from rabbitmq.middleware import MessageMiddlewareExchangeManual, MessageMiddlewareQueueManual
from dtos.dto import TransactionItemBatchDTO, BatchType
from common.graceful_shutdown import GracefulShutdown
from client_routing.client_routing import ClientRouter
from checkpoint_handler.checkpoint_handler import CheckpointHandler
from healthchecker.healthchecker import HealthChecker
from best_selling.utils import BestSellingAggregatorUtils


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ItemMetrics:
    def __init__(self, item_id: str):
        self.item_id = item_id
        self.sellings_qty = 0
        self.profit_sum = 0.0


class BestSellingAggregatorNode:
    def __init__(self):
        self.shutdown = GracefulShutdown()
        self.shutdown.register_callback(self._on_shutdown_signal)
        
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.node_id = int(os.getenv('AGGREGATOR_NODE_ID', '0'))
        self.total_years = int(os.getenv('TOTAL_YEARS', '2'))
        self.total_groupby_nodes_per_year = int(os.getenv('TOTAL_GROUPBY_NODES', '4'))
        self.expected_sources = self.total_years * self.total_groupby_nodes_per_year
        self.checkpoint_dir = os.getenv('CHECKPOINT_DIR', f'/app/server/logs/best_selling_aggregator_final/checkpoints')
        
        self.client_router = ClientRouter(int(os.getenv('TOTAL_JOIN_NODES', '1')), node_prefix="join_node")
        
        self.month_selling_candidates_by_client = defaultdict(lambda: defaultdict(list))
        self.month_profit_candidates_by_client = defaultdict(lambda: defaultdict(list))
        self.eof_selling_count_by_client = defaultdict(int)
        self.eof_profit_count_by_client = defaultdict(int)
        
        self.outgoing_counter_by_client = defaultdict(int)
        self.pending_rollbacks = {}

        self.best_selling_utils = BestSellingAggregatorUtils(self)
        self.checkpoint_handler = CheckpointHandler(
            checkpoint_dir=self.checkpoint_dir,
            strategy=self.best_selling_utils,
            checkpont_interval=4,
            outgoing_counter_by_client=self.outgoing_counter_by_client,
            extra_id=int(self.node_id)
        )
        
        self.health_server = HealthChecker(port=int(os.getenv('HEALTH_PORT', '9999')))
        self.health_server.start()
        
        logger.info(f"BestSellingAggregatorFinal inicializado. Recuperando estado...")

        self.checkpoint_handler.recover_from_checkpoint()
        
        self._execute_pending_rollbacks()
        
        self._setup_middleware()
        
    def _execute_pending_rollbacks(self):
        if self.pending_rollbacks:
            logger.warning("Detectadas transacciones incompletas. Aplicando Rollbacks...")
            for client_id, reset_value in self.pending_rollbacks.items():
                current = self.outgoing_counter_by_client.get(client_id, 0)
                logger.warning(f"  -> Cliente {client_id}: Rebobinando contador {current} -> {reset_value}")
                self.outgoing_counter_by_client[client_id] = reset_value
            self.pending_rollbacks.clear()
        else:
            logger.info("Estado consistente. No se requieren rollbacks.")

    def _setup_middleware(self):
        input_exchange = os.getenv('INPUT_EXCHANGE', 'best_selling_to_final.exchange')
        input_queue = os.getenv('INPUT_QUEUE', 'best_selling_final')
        
        self.input_middleware = MessageMiddlewareQueueManual(
            host=self.rabbitmq_host,
            queue_name=input_queue,
            exchange_name=input_exchange,
            routing_keys=['top_selling.data', 'top_profit.data']
        )
        
        self.input_middleware.shutdown = self.shutdown
        
        output_exchange = os.getenv('OUTPUT_EXCHANGE', 'join.exchange')
        
        route_keys = []
        route_keys.extend(self.client_router.get_all_routing_keys('q2_best_selling.data'))
        route_keys.extend(self.client_router.get_all_routing_keys('q2_most_profit.data'))
        
        self.output_middleware = MessageMiddlewareExchangeManual(
            host=self.rabbitmq_host,
            exchange_name=output_exchange,
            route_keys=route_keys
        )
        
        self.output_middleware.shutdown = self.shutdown
                
        if hasattr(self.input_middleware, 'shutdown'):
            self.input_middleware.shutdown = self.shutdown
        if hasattr(self.output_middleware, 'shutdown'):
            self.output_middleware.shutdown = self.shutdown
    
    def _on_shutdown_signal(self):
        logger.info("Señal de shutdown recibida")
        if self.input_middleware:
            self.input_middleware.stop_consuming()

    
    def handle_eof(self, routing_key: str, client_id: str, message_id: str) -> bool:
        try:
            logger.info(f"EOF recibido para cliente {client_id}, routing_key={routing_key}")
            
            if 'top_selling' in routing_key:
                self.eof_selling_count_by_client[client_id] += 1
                logger.info(f"EOF selling #{self.eof_selling_count_by_client[client_id]}/{self.expected_sources} para cliente {client_id}")
                
            elif 'top_profit' in routing_key:
                self.eof_profit_count_by_client[client_id] += 1
                logger.info(f"EOF profit #{self.eof_profit_count_by_client[client_id]}/{self.expected_sources} para cliente {client_id}")
            
            selling_count = self.eof_selling_count_by_client[client_id]
            profit_count = self.eof_profit_count_by_client[client_id]
            
            if selling_count == self.expected_sources and profit_count == self.expected_sources:
                logger.info(f"TODOS los EOFs recibidos para cliente {client_id} ({selling_count} selling + {profit_count} profit)")                
                self._send_final_results(client_id, message_id)
                
                self.checkpoint_handler.save_eof_checkpoint(client_id, message_id)

                self.clean_client(client_id)
                logger.info(f"Cliente {client_id} completado y limpiado")
            else:
                logger.info(f"Esperando más EOFs: {selling_count}/{self.expected_sources} selling, {profit_count}/{self.expected_sources} profit")
            
            return False
            
        except Exception as e:
            logger.error(f"Error manejando EOF: {e}")
            return False
        
    def clean_client(self, client_id: str):
        logger.info(f"Limpiando estado para cliente {client_id}")
        try:
            if client_id in self.eof_selling_count_by_client:
                del self.eof_selling_count_by_client[client_id]
                logger.debug(f"eoF selling eliminado para cliente {client_id}")
            else:
                logger.debug(f"No había EOF selling para cliente {client_id}")
        except Exception as e:
            logger.warning(f"Error eliminando eof_selling_count_by_client[{client_id}]: {e}")

        try:
            if client_id in self.eof_profit_count_by_client:
                del self.eof_profit_count_by_client[client_id]
                logger.debug(f"eoF profit eliminado para cliente {client_id}")
            else:
                logger.debug(f"No había EOF profit para cliente {client_id}")
        except Exception as e:
            logger.warning(f"Error eliminando eof_profit_count_by_client[{client_id}]: {e}")

        try:
            if client_id in self.month_selling_candidates_by_client:
                del self.month_selling_candidates_by_client[client_id]
                logger.debug(f"month_selling_candidates eliminado para cliente {client_id}")
            else:
                logger.debug(f"No había month_selling_candidates para cliente {client_id}")
        except Exception as e:
            logger.warning(f"Error eliminando month_selling_candidates_by_client[{client_id}]: {e}")

        try:
            if client_id in self.month_profit_candidates_by_client:
                del self.month_profit_candidates_by_client[client_id]
                logger.debug(f"month_profit_candidates eliminado para cliente {client_id}")
            else:
                logger.debug(f"No había month_profit_candidates para cliente {client_id}")
        except Exception as e:
            logger.warning(f"Error eliminando month_profit_candidates_by_client[{client_id}]: {e}")

        try:
            if client_id in self.outgoing_counter_by_client:
                del self.outgoing_counter_by_client[client_id]
                logger.debug(f"outgoing_counter eliminado para cliente {client_id}")
            else:
                logger.debug(f"No había outgoing_counter para cliente {client_id}")
        except Exception as e:
            logger.warning(f"Error eliminando outgoing_counter_by_client[{client_id}]: {e}")

        try:
            if client_id in self.pending_rollbacks:
                del self.pending_rollbacks[client_id]
                logger.debug(f"pending_rollback eliminado para cliente {client_id}")
            else:
                logger.debug(f"No había pending_rollback para cliente {client_id}")
        except Exception as e:
            logger.warning(f"Error eliminando pending_rollbacks[{client_id}]: {e}")

        logger.info(f"Limpieza completada para cliente {client_id}")
        
    def clean_all_clients(self):
        """Limpia el estado de TODOS los clientes"""
        logger.info("Limpiando estado de TODOS los clientes")
        
        try:
            self.eof_selling_count_by_client.clear()
            logger.debug("Todos los EOF selling eliminados")
        except Exception as e:
            logger.warning(f"Error limpiando eof_selling_count_by_client: {e}")

        try:
            self.eof_profit_count_by_client.clear()
            logger.debug("Todos los EOF profit eliminados")
        except Exception as e:
            logger.warning(f"Error limpiando eof_profit_count_by_client: {e}")

        try:
            self.month_selling_candidates_by_client.clear()
            logger.debug("Todos los month_selling_candidates eliminados")
        except Exception as e:
            logger.warning(f"Error limpiando month_selling_candidates_by_client: {e}")

        try:
            self.month_profit_candidates_by_client.clear()
            logger.debug("Todos los month_profit_candidates eliminados")
        except Exception as e:
            logger.warning(f"Error limpiando month_profit_candidates_by_client: {e}")

        try:
            self.outgoing_counter_by_client.clear()
            logger.debug("Todos los outgoing_counter eliminados")
        except Exception as e:
            logger.warning(f"Error limpiando outgoing_counter_by_client: {e}")

        try:
            self.pending_rollbacks.clear()
            logger.debug("Todos los pending_rollbacks eliminados")
        except Exception as e:
            logger.warning(f"Error limpiando pending_rollbacks: {e}")

        logger.info("Limpieza global completada - todos los clientes eliminados")
        
    def create_headers(self, client_id: Optional[int], message_id: Optional[int]) -> Dict[str, Any]:
        headers = {}
        if client_id is not None and message_id is not None:
            return {'client_id': client_id,
                    'message_id': message_id
                    }
        return {}

    def _send_final_results(self, client_id: str, original_message_id: str):
        best_selling, most_profit = self.best_selling_utils.calculate_global_top1(client_id)        
        
        self.checkpoint_handler.start_batch_transaction(client_id, "best_selling")
        routing_key = self.client_router.get_routing_key(client_id, 'q2_best_selling.data')
        self._send_data_batch(client_id, best_selling, "sellings_qty", routing_key)
        self._send_eof(client_id, routing_key)
        self.checkpoint_handler.commit_batch_transaction(client_id, "best_selling")

        self.checkpoint_handler.start_batch_transaction(client_id, "most_profit")
        routing_key = self.client_router.get_routing_key(client_id, 'q2_most_profit.data')
        self._send_data_batch(client_id, most_profit, "profit_sum", routing_key)
        self._send_eof(client_id, routing_key)
        self.checkpoint_handler.commit_batch_transaction(client_id, "most_profit")

    def _send_data_batch(self, client_id: str, data: Dict, metric: str, routing_key: str):
        csv_content = self.best_selling_utils.generate_top1_csv(data, metric)
        unique_id = self.checkpoint_handler.get_next_id_in_memory(client_id)
        
        dto = TransactionItemBatchDTO(csv_content, BatchType.RAW_CSV)
        self.output_middleware.send(
            dto.to_bytes_fast(),
            routing_key=routing_key,
            headers={'client_id': client_id, 'message_id': unique_id}
        )
        
    def _send_eof(self, client_id: str, routing_key: str, eof_type: Optional[int] = 1):
        
        if eof_type == 1:
            unique_id = self.checkpoint_handler.get_next_id_in_memory(client_id)
        else:
            unique_id = 0
        dto = TransactionItemBatchDTO(f"EOF:{eof_type}", BatchType.EOF)
        self.output_middleware.send(
            dto.to_bytes_fast(),
            routing_key=routing_key,
            headers={'client_id': client_id, 'message_id': unique_id}
        )
        logger.info(f"Enviado EOF {routing_key} (ID={unique_id})")

    def process_message(self, message: bytes, routing_key: str, client_id: str, message_id: str) -> bool:
        try:
            if self.shutdown.is_shutting_down():
                logger.warning("Shutdown en progreso")
                return (True, False)
            
            dto = TransactionItemBatchDTO.from_bytes_fast(message)
            
            if dto.batch_type == BatchType.EOF:
                if dto.data.startswith("EOF:2") or dto.data.startswith("EOF:3"):
                    if dto.data.startswith("EOF:2"):
                        logger.info(f"EOF tipo 2 recibido, limpiando nodos del cliente {client_id}")
                        eof_type = 2
                    else:
                        logger.info(f"EOF tipo 3 recibido, formateando nodos")
                        eof_type = 3
                        
                    routing_key = self.client_router.get_routing_key(client_id, 'q2_best_selling.data')
                    self._send_eof(client_id, routing_key, eof_type=eof_type)
                    
                    routing_key = self.client_router.get_routing_key(client_id, 'q2_most_profit.data')
                    self._send_eof(client_id, routing_key, eof_type=eof_type)
                    if eof_type == 2:
                        self.clean_client(client_id)
                    else:
                        self.clean_all_clients()

                        
                    return (False, True)
                
                self.handle_eof(routing_key, client_id, message_id)

                lines = [f"EOF:{routing_key}"]
                self.checkpoint_handler.save_message_checkpoint(
                    client_id=client_id,
                    message_id=message_id,
                    csv_lines=lines
                )
                
                return (False, True) 
            
            if dto.batch_type == BatchType.RAW_CSV:
                lines = [line.strip() for line in dto.data.split('\n') if line.strip()]
                
                for line in lines:
                    self.best_selling_utils.process_csv_line(line, routing_key, client_id)
                
                self.checkpoint_handler.save_message_checkpoint(
                    client_id=client_id,
                    message_id=message_id,
                    csv_lines=lines
                )
                return (False, True) 
            
        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")
            return (False, False)

    def parse_message_headers(self, properties) -> Tuple[str, str]:
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
            
            should_stop, should_ack = self.process_message(body,routing_key, client_id, message_id)
            if should_ack:
                ch.basic_ack(delivery_tag=method.delivery_tag)
            if should_stop:
                logger.info("Shutdown solicitado - deteniendo consuming")
                ch.stop_consuming()
        except Exception as e:
            logger.error(f"Error en callback: {e}")
    
    def start(self):
        try:
            logger.info("Iniciando BestSellingAggregatorFinal...")
            self.input_middleware.start_consuming(self.on_message_callback)
        except KeyboardInterrupt:
            logger.info("Detenido manualmente")
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
        aggregator = BestSellingAggregatorNode()
        aggregator.start()
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error fatal: {e}")
        sys.exit(1)