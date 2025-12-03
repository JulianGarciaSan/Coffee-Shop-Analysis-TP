from collections import defaultdict
import logging
import os
import time
from typing import Dict, Any, List
from rabbitmq.middleware import MessageMiddlewareQueueManual, MessageMiddlewareExchangeManual
from dtos.dto import TransactionBatchDTO, BatchType
from .base_configurators import GroupByConfigurator

logger = logging.getLogger(__name__)


class TopCustomerConfigurator(GroupByConfigurator):
    def __init__(self, rabbitmq_host: str, output_exchange: str, outgoing_counter_by_client: Dict[str, int] = None):
        super().__init__(rabbitmq_host, output_exchange)
        self.input_queue_name = os.getenv('INPUT_QUEUE', 'year_filtered_q4')
        self.total_groupby_nodes = int(os.getenv('TOTAL_GROUPBY_NODES', '3'))
        self.topk_node_id = int(os.getenv('TOPK_NODE_ID', '1'))
        aggregator_ids_str = os.getenv('TOPK_AGGREGATORS_IDS', '3,4')
        self.topk_aggregators_ids: List[int] = [int(x.strip()) for x in aggregator_ids_str.split(',')]
        self.outgoing_counter_by_client = outgoing_counter_by_client or defaultdict(int)

        
        logger.info(f"TopCustomerConfigurator inicializado:")
        logger.info(f"  Input Queue: {self.input_queue_name}")
        logger.info(f"  Total GroupBy nodes: {self.total_groupby_nodes}")

    def create_input_middleware(self):
        middleware = MessageMiddlewareQueueManual(
            host=self.rabbitmq_host,
            queue_name=self.input_queue_name
        )
        logger.info(f"  Input: Queue {self.input_queue_name}")
        return middleware

    def create_output_middlewares(self) -> Dict[str, Any]:
        route_keys = [f'top_customers_aggregator_{i}' for i in self.topk_aggregators_ids]        
        output_middleware = MessageMiddlewareExchangeManual(
            host=self.rabbitmq_host,
            exchange_name=self.output_exchange,
            route_keys=route_keys,
            queue_name=f'groupby.top_customers.node.{self.topk_node_id}'
        )
        
        logger.info(f"  Output exchange: {self.output_exchange}")
        logger.info(f"  Routing keys: {route_keys}")
        return {"output": output_middleware}

    def handle_eof(self, dto: TransactionBatchDTO, middlewares: dict, strategy, client_id: str, message_id: str) -> bool:
        logger.info(f"EOF recibido de cliente '{client_id}' con message_id '{message_id}'")
        
        try:
            self._send_data_by_aggregator(middlewares["output"], strategy, client_id, message_id)
            self._send_eof_broadcast(middlewares["output"], client_id, message_id)
        except Exception as e:
            logger.error(f"Error procesando EOF para cliente {client_id}: {e}")
            raise
        
        return False

    def _shard_store_to_aggregator(self, store_id: str) -> int:
        """Sharding determinístico: store_id % cantidad_de_aggregators, mapeado a IDs específicos"""
        try:
            store_num = int(store_id.replace('store_', ''))
        except ValueError:
            store_num = hash(store_id)
        
        # Mapear al índice dentro de la lista de aggregators
        aggregator_index = store_num % len(self.topk_aggregators_ids)
        return self.topk_aggregators_ids[aggregator_index]

    def generate_next_message_id(self, client_id: str) -> int:
        """
        Genera ID único por mensaje para este cliente.
        Determinístico porque el contador se reconstruye desde el checkpoint.
        """
        self.outgoing_counter_by_client[client_id] += 1
        return int(self.topk_node_id) * 1000000 + self.outgoing_counter_by_client[client_id]
    
    def _send_data_by_aggregator(self, output_middleware, strategy, client_id, original_message_id):
        """
        Agrupa stores por aggregator y envía UN mensaje por aggregator.
        """
        store_user_purchases_by_client = getattr(strategy, 'store_user_purchases_by_client', {})
        client_data = store_user_purchases_by_client.get(client_id, {})
        
        if not client_data:
            logger.warning(f"No hay datos para client_id={client_id}")
            return
        
        stores_by_aggregator = defaultdict(list)
        for store_id in sorted(client_data.keys()):
            aggregator_id = self._shard_store_to_aggregator(store_id)
            stores_by_aggregator[aggregator_id].append(store_id)
        
        logger.info(f"Distribución: {len(stores_by_aggregator)} aggregators recibirán datos")
        
        for aggregator_id, stores in sorted(stores_by_aggregator.items()):
            csv_lines = ["store_id,user_id,purchases_qty"]
            
            for store_id in stores:
                user_purchases = client_data[store_id]
                for user_purchase in user_purchases.values():
                    csv_lines.append(user_purchase.to_csv_line(store_id))
            
            
            outgoing_message_id = self.generate_next_message_id(original_message_id)
            
            routing_key = f'top_customers_aggregator_{aggregator_id}'
            batch_csv = '\n'.join(csv_lines)
            result_dto = TransactionBatchDTO(batch_csv, BatchType.RAW_CSV)
            
            output_middleware.send(
                result_dto.to_bytes_fast(),
                routing_key,
                headers={'client_id': client_id, 'message_id': outgoing_message_id}
            )

    def _send_eof_broadcast(self, output_middleware, client_id, original_message_id):
        """Envía UN EOF a todos los aggregators."""
        logger.info(f"Enviando EOF a {len(self.topk_aggregators_ids)} aggregators")
        
        for aggregator_id in self.topk_aggregators_ids:
            eof_message_id = self.generate_next_message_id(original_message_id) + 1
            routing_key = f'top_customers_aggregator_{aggregator_id}'
            eof_dto = TransactionBatchDTO(f"EOF:{client_id}", BatchType.EOF)
            output_middleware.send(
                eof_dto.to_bytes_fast(),
                routing_key,
                headers={'client_id': client_id, 'message_id': eof_message_id}
            )
        
        logger.info(f"EOF enviado a todos los aggregators")

    def get_strategy_config(self) -> dict:
        return {
            'input_queue_name': self.input_queue_name,
            'outgoing_counter_by_client': self.outgoing_counter_by_client
        }