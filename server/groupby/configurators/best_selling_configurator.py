# best_selling_configurator.py
from collections import defaultdict
import logging
import os
import threading
from typing import Dict, Any, Optional
import time
from rabbitmq.middleware import MessageMiddlewareExchangeManual, MessageMiddlewareQueue,MessageMiddlewareQueueManual
from dtos.dto import TransactionItemBatchDTO, BatchType, CoordinationMessageDTO
from .base_configurators import GroupByConfigurator

logger = logging.getLogger(__name__)


class BestSellingConfigurator(GroupByConfigurator):
    def __init__(self, rabbitmq_host: str, output_exchange: str,outgoing_counter_by_client: Dict[str, int] = None):
        super().__init__(rabbitmq_host, output_exchange)
        self.year = os.getenv('AGGREGATOR_YEAR', '2024')
        self.input_exchange = os.getenv('INPUT_EXCHANGE', 'groupby_input.exchange')
        
        self.node_id = int(os.getenv('GROUPBY_NODE_ID', '0'))
        self.total_groupby_nodes = int(os.getenv('TOTAL_GROUPBY_NODES', '4'))
        self.outgoing_counter_by_client = outgoing_counter_by_client or defaultdict(int)
        self.node_name = f'groupby_{self.year}_node_{self.node_id}'
        self.input_queue_name = f"best_selling_{self.year}_node_{self.node_id}"
        
        self.message_id = 0
        
        self.dto_helper = TransactionItemBatchDTO("", BatchType.RAW_CSV)
        
        logger.info(f"BestSellingConfigurator inicializado:")
        logger.info(f"  Node: {self.node_name}")
        logger.info(f"  Year: {self.year}")
        logger.info(f"  Total nodos: {self.total_groupby_nodes}")
        logger.info(f"  Input Queue: {self.input_queue_name}")
        
    
    def create_input_middleware(self):
        routing_key = f"groupby_{self.year}_node_{self.node_id}"
        
        middleware = MessageMiddlewareQueueManual(
            host=self.rabbitmq_host,
            queue_name=self.input_queue_name,
            exchange_name=self.input_exchange,
            routing_keys=[routing_key]
        )
        
        logger.info(f"  Escuchando routing_key: {routing_key}")
        return middleware
    
    def create_output_middlewares(self) -> Dict[str, Any]:
        output_middleware = MessageMiddlewareExchangeManual(
            host=self.rabbitmq_host,
            exchange_name=self.output_exchange, 
            route_keys=['top_selling.data', 'top_profit.data']
        )
        
        logger.info(f"  Output exchange: {self.output_exchange}")
        logger.info(f"  Enviando directo a Aggregator Final")
        
        return {"output": output_middleware}
    

    def handle_eof(self, dto: TransactionItemBatchDTO, middlewares: dict, strategy, client_id: str, message_id: str,checkpoint_handler, eof_type: Optional[int]=1) -> bool:
        logger.info(f"EOF recibido para cliente '{client_id}'")
        
        try:
            if eof_type == 2 or eof_type == 3:
                logger.info(f"EOF:{eof_type} recibido para cliente '{client_id}', no se envían datos agregados")
                self._send_eof_to_aggregator(middlewares["output"], client_id, message_id,checkpoint_handler, eof_type=eof_type)
                if eof_type == 2:
                    strategy.clean_client_data(client_id)
                else:
                    strategy.clean_all_data()
                return False
            
            checkpoint_handler.start_batch_transaction(client_id, "q2")

            self._calculate_and_send_top1(middlewares["output"], strategy, client_id, message_id,checkpoint_handler)
            
            self._send_eof_to_aggregator(middlewares["output"], client_id, message_id,checkpoint_handler)
            
            checkpoint_handler.commit_batch_transaction(client_id, "q2")

            
            month_item_aggregations = getattr(strategy, 'month_item_aggregations_by_client', {})
            if client_id in month_item_aggregations:
                del month_item_aggregations[client_id]
                logger.info(f"Memoria limpiada para cliente {client_id}")
            
            return False
            
        except Exception as e:
            logger.error(f"Error manejando EOF para client_id={client_id}: {e}")
            return False

    def _send_eof_to_aggregator(self, output_middleware, client_id, message_id,checkpoint_handler, eof_type: Optional[int]=1):
        """Envía EOF al Aggregator Final"""
        
        if eof_type == 2 or eof_type == 3:
            unique_id = 0
        else:
            unique_id = checkpoint_handler.get_next_id_in_memory(client_id)
            
        logger.info(f"Enviando EOF:{eof_type} al Aggregator Final")
        eof_dto = TransactionItemBatchDTO(f"EOF:{eof_type}", BatchType.EOF)

        headers = self.create_headers(client_id, unique_id)
        output_middleware.send(
            eof_dto.to_bytes_fast(),
            routing_key='top_selling.data',
            headers=headers
        )
        logger.info(f"EOF:{eof_type} top_selling enviado (ID={unique_id} para cliente {client_id})")
        
        if eof_type == 2 or eof_type == 3:
            unique_id = 0
        else:
            unique_id = checkpoint_handler.get_next_id_in_memory(client_id)
        
        headers = self.create_headers(client_id, unique_id)
        output_middleware.send(
            eof_dto.to_bytes_fast(),
            routing_key='top_profit.data',
            headers=headers
        )
        logger.info(f"EOF:{eof_type} top_profit enviado (ID={unique_id} para cliente {client_id})")
    
    def _calculate_and_send_top1(self, output_middleware, strategy, client_id, message_id, checkpoint_handler):
        """
        Calcula el top 1 LOCAL y envía al Aggregator Final
        """
        month_item_aggregations_by_client = getattr(strategy, 'month_item_aggregations_by_client', {})
        client_data = month_item_aggregations_by_client.get(client_id, {})
        
        if not client_data:
            return
        
        for year_month in sorted(client_data.keys()):
            items = client_data[year_month]
            valid_items = [item for item in items.values() if item is not None]
            
            if not valid_items:
                continue
            
            # Top selling
            top_selling_item = max(valid_items, 
                                key=lambda x: (x.sellings_qty, -int(x.item_id) if x.item_id.isdigit() else 0))
            
            selling_csv = f"created_at,item_id,sellings_qty\n"
            selling_csv += f"{year_month},{top_selling_item.item_id},{top_selling_item.sellings_qty}"
            
            # Generar ID único usando client_id
            unique_id = checkpoint_handler.get_next_id_in_memory(client_id)
            selling_dto = TransactionItemBatchDTO(selling_csv, BatchType.RAW_CSV)
            output_middleware.send(
                selling_dto.to_bytes_fast(),
                routing_key='top_selling.data',
                headers=self.create_headers(client_id, unique_id)
            )
            logger.info(f"Enviado selling {year_month} (ID={unique_id} para cliente {client_id})")
            # Top profit
            top_profit_item = max(valid_items,
                                key=lambda x: (x.profit_sum, -int(x.item_id) if x.item_id.isdigit() else 0))
            
            profit_csv = f"created_at,item_id,profit_sum\n"
            profit_csv += f"{year_month},{top_profit_item.item_id},{top_profit_item.profit_sum:.2f}"
            
            # Generar siguiente ID único
            unique_id = checkpoint_handler.get_next_id_in_memory(client_id)
            profit_dto = TransactionItemBatchDTO(profit_csv, BatchType.RAW_CSV)
            output_middleware.send(
                profit_dto.to_bytes_fast(),
                routing_key='top_profit.data',
                headers=self.create_headers(client_id, unique_id)
            )
            logger.info(f"Enviado profit {year_month} (ID={unique_id} para cliente {client_id})")
            time.sleep(5)

        logger.info(f"Top 1 local enviado para cliente {client_id}")
    
    def get_strategy_config(self) -> dict:
        return {
            'input_queue_name': self.input_queue_name,
            'year': self.year,
            'outgoing_counter_by_client': self.outgoing_counter_by_client
        }
    
