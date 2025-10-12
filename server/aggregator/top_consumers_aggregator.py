import logging
import os
import sys
from typing import Dict
from collections import defaultdict
from rabbitmq.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
from dtos.dto import TransactionBatchDTO, BatchType
from common.graceful_shutdown import GracefulShutdown
from dtos.dto import TransactionBatchDTO, BatchType, CoordinationMessageDTO
from  client_routing.client_routing import ClientRouter
import threading
from coordinator.coordinator import PeerCoordinator


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TopCustomersAggregatorNode:
    def __init__(self):
        self._init_shutdown()
        self._init_config()
        self._init_data_structures()
        self._init_coordination_if_needed()
        self._init_middlewares()

    def _init_shutdown(self):
        self.shutdown = GracefulShutdown()
        self.shutdown.register_callback(self._on_shutdown_signal)

    def _init_config(self):
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.mode = os.getenv('AGGREGATOR_MODE', 'intermediate')
        logger.info(f"Inicializando TopCustomersAggregatorNode con modo: {self.mode}")

    def _init_data_structures(self):
        self.store_user_purchases: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self.input_middleware = None
        self.output_middleware = None
        self.eof_count_by_client: Dict[str, int] = defaultdict(int)
        if self.mode == 'final':
            self.total_intermediate = int(os.getenv('TOTAL_TOPK_NODES', '2'))
            self.final_eof_count_by_client = defaultdict(int)
            logger.info(f"[FINAL] Esperando {self.total_intermediate} nodos intermediate")
            total_join_nodes = int(os.getenv('TOTAL_JOIN_NODES', '2'))
            self.client_router = ClientRouter(total_join_nodes=total_join_nodes)

    def _init_coordination_if_needed(self):
        if self.mode != 'intermediate':
            return
        topk_node_id = os.getenv('TOPK_NODE_ID')
        if topk_node_id is not None:
            self.node_id = topk_node_id
        else:
            self.node_id = os.getenv('NODE_ID', '1')
        self.total_nodes = int(os.getenv('TOTAL_TOPK_NODES', '2'))
        
        total_stores = 10
        stores_per_node = total_stores // self.total_nodes
        extra_stores = total_stores % self.total_nodes
        try:
            node_num = int(self.node_id)
        except ValueError:
            node_num = int(str(self.node_id).split('_')[-1])
        
        if node_num <= extra_stores:
            self.expected_eof_per_client = stores_per_node + 1
        else:
            self.expected_eof_per_client = stores_per_node
            
        logger.info(f"[INTERMEDIATE] TopK node {self.node_id} espera {self.expected_eof_per_client} EOF por cliente")

    def _init_middlewares(self):
        try:
            self._setup_input_middleware()
            self._setup_output_middleware()
            if hasattr(self.input_middleware, 'shutdown'):
                self.input_middleware.shutdown = self.shutdown
            if hasattr(self.output_middleware, 'shutdown'):
                self.output_middleware.shutdown = self.shutdown
        except Exception as e:
            logger.error(f"Error durante la configuración de middlewares: {e}")
            raise
            
    def _on_shutdown_signal(self):
        logger.info("Señal de shutdown recibida en TopCustomersAggregator")
        if self.input_middleware:
            self.input_middleware.stop_consuming()
                
    def _setup_input_middleware(self):
        if self.mode == 'intermediate':
            input_exchange = os.getenv('INPUT_EXCHANGE', 'aggregated.exchange')
            queue_name = os.getenv('INPUT_QUEUE', 'aggregated_data')

            total_stores = 10
            stores_per_node = total_stores // self.total_nodes
            extra_stores = total_stores % self.total_nodes

            try:
                node_num = int(self.node_id)
            except ValueError:
                node_num = int(str(self.node_id).split('_')[-1])

            start_store = (node_num - 1) * stores_per_node + min(node_num - 1, extra_stores)
            end_store = start_store + stores_per_node + (1 if node_num <= extra_stores else 0)

            routing_keys = [f"store.{i}" for i in range(start_store + 1, end_store + 1)]

            self.input_middleware = MessageMiddlewareQueue(
                host=self.rabbitmq_host,
                queue_name=queue_name,  
                exchange_name=input_exchange,
                routing_keys=routing_keys 
            )
            logger.info(f"  Input Exchange: {input_exchange}")
            logger.info(f"  Node {self.node_id} procesa stores: {start_store + 1}-{end_store}")
            logger.info(f"  Routing Keys: {routing_keys}")
            
        elif self.mode == 'final':
            input_exchange = os.getenv('INPUT_EXCHANGE', 'topk.exchange')
            input_queue = os.getenv('INPUT_QUEUE', 'topk_final')
            
            self.input_middleware = MessageMiddlewareQueue(
                host=self.rabbitmq_host,
                queue_name=input_queue,
                exchange_name=input_exchange,
                routing_keys=['topk.local.data']
            )
            logger.info(f"  Input Exchange: {input_exchange}")
            logger.info(f"  Input Queue: {input_queue}")
            logger.info(f"  Routing key: topk.local.data")
    
    def _setup_output_middleware(self):
        if self.mode == 'intermediate':
            output_exchange = os.getenv('OUTPUT_EXCHANGE', 'topk.exchange')
            route_keys = ['topk.local.data']
            
            self.output_middleware = MessageMiddlewareExchange(
                host=self.rabbitmq_host,
                exchange_name=output_exchange,
                route_keys=route_keys
            )
            logger.info(f"  Output Exchange: {output_exchange}")
            
        elif self.mode == 'final':
            output_exchange = os.getenv('OUTPUT_EXCHANGE', 'join.exchange')
            route_keys = ['top_customers.data']
            
            self.output_middleware = MessageMiddlewareExchange(
                host=self.rabbitmq_host,
                exchange_name=output_exchange,
                route_keys=route_keys
            )
            logger.info(f"  Output Exchange: {output_exchange}")
    
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
    
    def generate_top3_csv(self) -> str:
        if not self.store_user_purchases:
            return "store_id,user_id,purchases_qty"
        
        csv_lines = ["store_id,user_id,purchases_qty"]
        
        for store_id in sorted(self.store_user_purchases.keys()):
            user_purchases = self.store_user_purchases[store_id]
            
            sorted_users = sorted(
                user_purchases.items(),
                key=lambda x: (-x[1], int(float(x[0].replace('.0', '')))),
            )
            
            top_3 = sorted_users[:3]
            
            for user_id, purchases_qty in top_3:
                csv_lines.append(f"{store_id},{user_id},{purchases_qty}")
        
        logger.info(f"Top 3 calculado para {len(self.store_user_purchases)} tiendas")
        return '\n'.join(csv_lines)
    
    def handle_eof(self, dto: TransactionBatchDTO, routing_key: str = None) -> bool:
        try:
            from dtos.dto import BatchType
            client_id = getattr(dto, 'client_id', 'default_client')
            
            if self.mode == 'intermediate':
                self.eof_count_by_client[client_id] += 1
                logger.info(f"EOF recibido para cliente {client_id} en routing_key {routing_key} (total: {self.eof_count_by_client[client_id]}/{self.expected_eof_per_client})")
                
                # Verificar si hemos recibido EOF de todas las stores asignadas
                if self.eof_count_by_client[client_id] >= self.expected_eof_per_client:
                    logger.info(f"EOF completo recibido para cliente {client_id}, enviando resultados downstream")
                    results_csv = self.generate_top3_csv()
                    result_dto = TransactionBatchDTO(results_csv, BatchType.RAW_CSV)
                    self.output_middleware.send(
                        result_dto.to_bytes_fast(),
                        routing_key='topk.local.data',
                        headers={'client_id': client_id}
                    )
                    eof_dto = TransactionBatchDTO(f"EOF:{client_id}", BatchType.EOF)
                    self.output_middleware.send(
                        eof_dto.to_bytes_fast(),
                        routing_key='topk.local.data',
                        headers={'client_id': client_id}
                    )
                    logger.info(f"Top 3 y EOF enviados para client_id={client_id}")
                    return False
                else:
                    logger.info(f"Esperando más EOF para cliente {client_id} ({self.eof_count_by_client[client_id]}/{self.expected_eof_per_client})")
                    return False
                
            elif self.mode == 'final':
                self.final_eof_count_by_client[client_id] += 1
                logger.info(f"EOF recibido: {self.final_eof_count_by_client[client_id]}/{self.total_intermediate} para client_id={client_id}")
                if self.final_eof_count_by_client[client_id] >= self.total_intermediate:
                    logger.info(f"[FINAL] Todos los intermediate terminaron - calculando Top 3 global para client_id={client_id}")
                    results_csv = self.generate_top3_csv()
                    result_dto = TransactionBatchDTO(results_csv, BatchType.RAW_CSV)
                    routing_key = self.client_router.get_routing_key(client_id, 'top_customers.data')
                    self.output_middleware.send(
                        result_dto.to_bytes_fast(),
                        routing_key=routing_key,
                        headers={'client_id': client_id}
                    )
                    eof_dto = TransactionBatchDTO(f"EOF:{client_id}", BatchType.EOF)
                    self.output_middleware.send(
                        eof_dto.to_bytes_fast(),
                        routing_key=routing_key,
                        headers={'client_id': client_id}
                    )
                    logger.info(f"[FINAL] Top 3 global enviado al JOIN para client_id={client_id} → {routing_key}")
                    logger.info(f"Total tiendas procesadas: {len(self.store_user_purchases)}")
                    # NO cerrar - continuar escuchando para otros clientes
                    return False
                return False
            return False
        except Exception as e:
            logger.error(f"Error manejando EOF: {e}")
            return False
    
    def process_message(self, message: bytes, routing_key: str = None, headers: dict = None) -> bool:
        try:
            if self.shutdown.is_shutting_down():
                logger.warning("Shutdown en progreso, ignorando mensaje")
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
                logger.warning("Shutdown solicitado, deteniendo")
                ch.stop_consuming()
                return

            routing_key = getattr(method, 'routing_key', None)
            headers = None
            if hasattr(properties, 'headers'):
                headers = properties.headers
            
            self.process_message(body, routing_key, headers)
            
        except Exception as e:
            logger.error(f"Error en callback: {e}")
    
    def start(self):
        try:
            mode_str = f"Intermediate (node {self.node_id})" if self.mode == 'intermediate' else "Final"
            logger.info(f"Iniciando TopCustomersAggregator {mode_str}...")
            
            if self.input_middleware is None:
                logger.error("input_middleware no está inicializado")
                raise RuntimeError("input_middleware no está inicializado")
                
            self.input_middleware.start_consuming(self.on_message_callback)
        except KeyboardInterrupt:
            logger.info("Aggregator detenido manualmente")
        except Exception as e:
            logger.error(f"Error durante el consumo: {e}")
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
            logger.error(f"Error durante cleanup: {e}")


if __name__ == "__main__":
    try:
        aggregator = TopCustomersAggregatorNode()
        aggregator.start()
        logger.info("BestSellingAggregator terminado exitosamente")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error fatal: {e}")
        sys.exit(1)