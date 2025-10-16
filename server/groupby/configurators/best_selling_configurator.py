# best_selling_configurator.py
import logging
import os
import threading
from typing import Dict, Any
from rabbitmq.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
from dtos.dto import TransactionItemBatchDTO, BatchType, CoordinationMessageDTO
from .base_configurators import GroupByConfigurator

logger = logging.getLogger(__name__)


class BestSellingConfigurator(GroupByConfigurator):
    def __init__(self, rabbitmq_host: str, output_exchange: str):
        super().__init__(rabbitmq_host, output_exchange)
        self.year = os.getenv('AGGREGATOR_YEAR', '2024')
        self.input_exchange = os.getenv('INPUT_EXCHANGE', 'groupby_input.exchange')
        
        self.node_id = int(os.getenv('GROUPBY_NODE_ID', '0'))
        self.total_groupby_nodes = int(os.getenv('TOTAL_GROUPBY_NODES', '4'))
        all_node_ids_str = os.getenv('ALL_NODE_IDS', '')
        all_node_ids = [nid.strip() for nid in all_node_ids_str.split(',')] if all_node_ids_str else [f'groupby_{self.year}_node_{self.node_id}']
        
        self.node_name = f'groupby_{self.year}_node_{self.node_id}'
        self.input_queue_name = f"best_selling_{self.year}_node_{self.node_id}"
        
        
        self.dto_helper = TransactionItemBatchDTO("", BatchType.RAW_CSV)
        
        logger.info(f"BestSellingConfigurator inicializado:")
        logger.info(f"  Node: {self.node_name}")
        logger.info(f"  Year: {self.year}")
        logger.info(f"  Total nodos: {self.total_groupby_nodes}")
        logger.info(f"  Input Queue: {self.input_queue_name}")
        
    
    def create_input_middleware(self):
        routing_key = f"groupby_{self.year}_node_{self.node_id}"
        
        middleware = MessageMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.input_queue_name,
            exchange_name=self.input_exchange,
            routing_keys=[routing_key]
        )
        
        logger.info(f"  Escuchando routing_key: {routing_key}")
        return middleware
    
    def create_output_middlewares(self) -> Dict[str, Any]:
        output_middleware = MessageMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name=self.output_exchange, 
            route_keys=['top_selling.data', 'top_profit.data']
        )
        
        logger.info(f"  Output exchange: {self.output_exchange}")
        logger.info(f"  Enviando directo a Aggregator Final")
        
        return {"output": output_middleware}
    
    def process_message(self, body: bytes, headers: dict = None) -> tuple:
        dto = TransactionItemBatchDTO.from_bytes_fast(body)
        
        client_id = 'default_client'
        if headers and 'client_id' in headers:
            client_id = headers['client_id']
            if isinstance(client_id, bytes):
                client_id = client_id.decode('utf-8')
        
        dto.client_id = str(client_id)
        
        is_eof = (dto.batch_type == BatchType.EOF)
        
        return (False, dto, is_eof) 
    
    def handle_eof(self, dto: TransactionItemBatchDTO, middlewares: dict, strategy) -> bool:
        client_id = getattr(dto, 'client_id', 'default_client')
        logger.info(f"EOF recibido para cliente '{client_id}'")
        
        try:
            self._calculate_and_send_top1(middlewares["output"], strategy, client_id)
            
            self._send_eof_to_aggregator(middlewares["output"], client_id)
            
            month_item_aggregations = getattr(strategy, 'month_item_aggregations_by_client', {})
            if client_id in month_item_aggregations:
                del month_item_aggregations[client_id]
                logger.info(f"Memoria limpiada para cliente {client_id}")
            
            return False
            
        except Exception as e:
            logger.error(f"Error manejando EOF para client_id={client_id}: {e}")
            return False
        
    def _send_eof_to_aggregator(self, output_middleware, client_id):
        """Envía EOF al Aggregator Final"""
        headers = {'client_id': client_id, 'groupby_node': self.node_name}
        
        eof_dto = TransactionItemBatchDTO(f"EOF:{self.node_name}", BatchType.EOF)
        
        output_middleware.send(
            eof_dto.to_bytes_fast(),
            routing_key='top_selling.data',
            headers=headers
        )
        
        output_middleware.send(
            eof_dto.to_bytes_fast(),
            routing_key='top_profit.data',
            headers=headers
        )
        
        logger.info(f"EOF enviado al Aggregator Final para cliente {client_id}")
    
    def _calculate_and_send_top1(self, output_middleware, strategy, client_id):
        """
        Calcula el top 1 LOCAL (de los items que procesó este nodo)
        y envía al Aggregator Final
        """
        month_item_aggregations_by_client = getattr(strategy, 'month_item_aggregations_by_client', {})
        client_data = month_item_aggregations_by_client.get(client_id, {})
        
        if not client_data:
            logger.warning(f"No hay datos para enviar para client_id={client_id}")
            return
        
        headers = {'client_id': client_id}
        
        logger.info(f"Calculando top 1 local para {len(client_data)} meses, cliente {client_id}")
        
        for year_month in sorted(client_data.keys()):
            items = client_data[year_month]
            
            valid_items = [item for item in items.values() if item is not None]
            
            if not valid_items:
                continue
            
            top_selling_item = max(valid_items, 
                                key=lambda x: (x.sellings_qty, -int(x.item_id) if x.item_id.isdigit() else 0))
            
            selling_csv = f"created_at,item_id,sellings_qty\n"
            selling_csv += f"{year_month},{top_selling_item.item_id},{top_selling_item.sellings_qty}"
            
            selling_dto = TransactionItemBatchDTO(selling_csv, BatchType.RAW_CSV)
            output_middleware.send(
                selling_dto.to_bytes_fast(),
                routing_key='top_selling.data',
                headers=headers
            )
            
            logger.info(f"✓ Top selling local {year_month}: item {top_selling_item.item_id} ({top_selling_item.sellings_qty} ventas)")
            
            top_profit_item = max(valid_items,
                                key=lambda x: (x.profit_sum, -int(x.item_id) if x.item_id.isdigit() else 0))
            
            profit_csv = f"created_at,item_id,profit_sum\n"
            profit_csv += f"{year_month},{top_profit_item.item_id},{top_profit_item.profit_sum:.2f}"
            
            profit_dto = TransactionItemBatchDTO(profit_csv, BatchType.RAW_CSV)
            output_middleware.send(
                profit_dto.to_bytes_fast(),
                routing_key='top_profit.data',
                headers=headers
            )
            
            logger.info(f"Top profit local {year_month}: item {top_profit_item.item_id} (${top_profit_item.profit_sum:.2f})")
        
        logger.info(f"Top 1 local enviado para cliente {client_id}")
        
        if client_id in month_item_aggregations_by_client:
            del month_item_aggregations_by_client[client_id]
            logger.info(f"Memoria limpiada para cliente {client_id}")
    
    def _on_all_acks_received(self, client_id: str, middlewares: dict):
        logger.info(f"Todos los ACKs recibidos para cliente {client_id}, propagando EOF")
        
        headers = {'client_id': client_id}
        
        eof_dto = TransactionItemBatchDTO(f"EOF:{client_id}", BatchType.EOF)
        
        middlewares["output"].send(
            eof_dto.to_bytes_fast(),
            routing_key='top_selling.data',
            headers=headers
        )
        
        middlewares["output"].send(
            eof_dto.to_bytes_fast(),
            routing_key='top_profit.data',
            headers=headers
        )
        
        logger.info(f"EOF propagado a Aggregator Final para cliente {client_id}")
    
    def get_strategy_config(self) -> dict:
        return {
            'input_queue_name': self.input_queue_name,
            'year': self.year
        }
    
    def close(self):
        logger.info("Cerrando BestSellingConfigurator...")
        
        self.coordination_running = False
        if self.coordination_queue:
            try:
                self.coordination_queue.stop_consuming()
                self.coordination_queue.close()
            except Exception as e:
                logger.error(f"Error cerrando coordination_queue: {e}")
        
        if self.coordinator:
            self.coordinator.close()
        
        logger.info("BestSellingConfigurator cerrado")