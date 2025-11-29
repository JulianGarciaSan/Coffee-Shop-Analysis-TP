from collections import defaultdict
import logging
import os
import time
from typing import Dict, Any
from rabbitmq.middleware import MessageMiddlewareQueueManual, MessageMiddlewareExchangeManual
from dtos.dto import TransactionBatchDTO, BatchType
from .base_configurators import GroupByConfigurator

logger = logging.getLogger(__name__)


class TopCustomerConfigurator(GroupByConfigurator):
    def __init__(self, rabbitmq_host: str, output_exchange: str):
        super().__init__(rabbitmq_host, output_exchange)
        self.input_queue_name = os.getenv('INPUT_QUEUE', 'year_filtered_q4')
        self.total_groupby_nodes = int(os.getenv('TOTAL_GROUPBY_NODES', '3'))
        self.topk_node_id = int(os.getenv('TOPK_NODE_ID', '1'))
        self.total_aggregator_nodes = int(os.getenv('TOTAL_TOPK_AGGREGATORS', '2'))
        
        logger.info(f"TopCustomerConfigurator inicializado:")
        logger.info(f"  Input Queue: {self.input_queue_name}")
        logger.info(f"  Total GroupBy nodes: {self.total_groupby_nodes}")
        logger.info(f"  Total Aggregator nodes: {self.total_aggregator_nodes}")

    def create_input_middleware(self):
        middleware = MessageMiddlewareQueueManual(
            host=self.rabbitmq_host,
            queue_name=self.input_queue_name
        )
        logger.info(f"  Input: Queue {self.input_queue_name}")
        return middleware

    def create_output_middlewares(self) -> Dict[str, Any]:
        route_keys = [f'top_customers_aggregator_{i}' for i in range(self.total_aggregator_nodes)]
        
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
        """Sharding determinístico: store_id % total_aggregators"""
        try:
            store_num = int(store_id.replace('store_', ''))
        except ValueError:
            store_num = hash(store_id)
        
        return store_num % self.total_aggregator_nodes

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
            
            outgoing_message_id = f"{original_message_id}:A{aggregator_id}"
            
            routing_key = f'top_customers_aggregator_{aggregator_id}'
            batch_csv = '\n'.join(csv_lines)
            result_dto = TransactionBatchDTO(batch_csv, BatchType.RAW_CSV)
            
            output_middleware.send(
                result_dto.to_bytes_fast(),
                routing_key,
                headers={'client_id': client_id, 'message_id': outgoing_message_id}
            )
            
            logger.info(f"Aggregator {aggregator_id}: {len(stores)} stores, {len(csv_lines)-1} líneas")

    def _send_eof_broadcast(self, output_middleware, client_id, original_message_id):
        """Envía UN EOF a todos los aggregators."""
        logger.info(f"Enviando EOF a {self.total_aggregator_nodes} aggregators")
        
        for aggregator_id in range(self.total_aggregator_nodes):
            outgoing_message_id = f"{original_message_id}:EOF"
            routing_key = f'top_customers_aggregator_{aggregator_id}'
            eof_dto = TransactionBatchDTO(f"EOF:{client_id}", BatchType.EOF)
            output_middleware.send(
                eof_dto.to_bytes_fast(),
                routing_key,
                headers={'client_id': client_id, 'message_id': outgoing_message_id}
            )
        
        logger.info(f"EOF enviado a todos los aggregators")

    def get_strategy_config(self) -> dict:
        return {
            'input_queue_name': self.input_queue_name,
        }