from collections import defaultdict
import logging
import os
from typing import Dict, Any
from rabbitmq.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
from dtos.dto import TransactionBatchDTO, BatchType
from .base_configurators import GroupByConfigurator

logger = logging.getLogger(__name__)


class TopCustomerConfigurator(GroupByConfigurator):
    def __init__(self, rabbitmq_host: str, output_exchange: str):
        super().__init__(rabbitmq_host, output_exchange)
        self.input_queue_name = os.getenv('INPUT_QUEUE', 'year_filtered_q4')
        self.total_groupby_nodes = int(os.getenv('TOTAL_GROUPBY_NODES', '3'))
        self.eof_count_by_client: Dict[str, int] = defaultdict(int)
        logger.info(f"TopCustomerConfigurator inicializado:")
        logger.info(f"  Input Queue: {self.input_queue_name}")
        logger.info(f"  Total nodos: {self.total_groupby_nodes}")

    def create_input_middleware(self):
        middleware = MessageMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.input_queue_name
        )
        logger.info(f"  Input: Queue {self.input_queue_name} (round-robin)")
        return middleware

    def create_output_middlewares(self) -> Dict[str, Any]:
        output_middleware = MessageMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name=self.output_exchange,
            route_keys=['store.*']
        )
        logger.info(f"  Output exchange: {self.output_exchange}")
        logger.info(f"  Routing pattern: store.* (por store_id)")
        return {"output": output_middleware}

    def process_message(self, body: bytes, headers: dict = None) -> tuple:
        dto = TransactionBatchDTO.from_bytes_fast(body)
        client_id = 'default_client'
        if headers and 'client_id' in headers:
            client_id = headers['client_id']
            if isinstance(client_id, bytes):
                client_id = client_id.decode('utf-8')
        dto.client_id = client_id
        is_eof = (dto.batch_type == BatchType.EOF)
        should_stop = False
        return (should_stop, dto, is_eof)

    def handle_eof(self, dto: TransactionBatchDTO, middlewares: dict, strategy) -> bool:
        client_id = getattr(dto, 'client_id', 'default_client')
        logger.info(f"EOF recibido de cliente '{client_id}'")
        
        self._send_data_by_store_for_client(middlewares["output"], strategy, client_id)
        self._send_eof_by_store_for_client(middlewares["output"], strategy, client_id)

        return False

    def _send_data_by_store_for_client(self, output_middleware, strategy, client_id):
        store_user_purchases_by_client = getattr(strategy, 'store_user_purchases_by_client', {})
        client_data = store_user_purchases_by_client.get(client_id, {})
        if not client_data:
            logger.warning(f"No hay datos locales para enviar para client_id={client_id}")
            return
        total_stores = len(client_data)
        logger.info(f"Enviando datos de {total_stores} stores para client_id={client_id}")
        for store_id in sorted(client_data.keys()):
            store_csv_lines = ["store_id,user_id,purchases_qty"]
            user_purchases = client_data[store_id]
            for user_purchase in user_purchases.values():
                store_csv_lines.append(user_purchase.to_csv_line(store_id))
            store_csv = '\n'.join(store_csv_lines)
            routing_key = f"store.{store_id}"
            result_dto = TransactionBatchDTO(store_csv, BatchType.RAW_CSV)
            output_middleware.send(result_dto.to_bytes_fast(), routing_key, headers={'client_id': client_id})
            logger.info(f"Store {store_id}: {len(store_csv_lines)-1} users '{routing_key}' para client_id={client_id}")

    def _send_eof_by_store_for_client(self, output_middleware, strategy, client_id):
        store_user_purchases_by_client = getattr(strategy, 'store_user_purchases_by_client', {})
        client_data = store_user_purchases_by_client.get(client_id, {})
        if not client_data:
            logger.warning(f"No hay datos para enviar EOF para client_id={client_id}")
            return
        
        total_stores = len(client_data)
        logger.info(f"Enviando EOF de {total_stores} stores para client_id={client_id}")
        
        for store_id in sorted(client_data.keys()):
            routing_key = f"store.{store_id}"
            eof_dto = TransactionBatchDTO(f"EOF:{client_id}", BatchType.EOF)
            output_middleware.send(eof_dto.to_bytes_fast(), routing_key, headers={'client_id': client_id})
            logger.info(f"EOF enviado para store {store_id} con routing key '{routing_key}' para client_id={client_id}")

    def get_strategy_config(self) -> dict:
        return {
            'input_queue_name': self.input_queue_name
        }

