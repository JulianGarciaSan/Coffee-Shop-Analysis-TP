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
            route_keys=['store.*', 'aggregated.eof'] 
        )
        
        logger.info(f"  Output exchange: {self.output_exchange}")
        logger.info(f"  Routing pattern: store.* (por store_id)")
        
        return {"output": output_middleware}
    
    def process_message(self, body: bytes) -> tuple:
        dto = TransactionBatchDTO.from_bytes_fast(body)
        
        logger.info(f"Mensaje recibido: batch_type={dto.batch_type}, tamaño={len(dto.data)} bytes")
        
        is_eof = (dto.batch_type == BatchType.EOF)
        should_stop = False
        
        return (should_stop, dto, is_eof)
    
    def handle_eof(self, dto: TransactionBatchDTO, middlewares: dict, strategy) -> bool:
        try:
            eof_data = dto.data.strip()
            counter = int(eof_data.split(':')[1]) if ':' in eof_data else 1
            
            logger.info(f"EOF recibido con counter={counter}, total={self.total_groupby_nodes}")
            
            # Obtener datos de la strategy y enviarlos por store
            self._send_data_by_store(middlewares["output"], strategy)
            
            # Lógica de EOF round-robin
            if counter < self.total_groupby_nodes:
                new_counter = counter + 1
                eof_dto = TransactionBatchDTO(f"EOF:{new_counter}", BatchType.EOF)
                
                if 'input_queue' in middlewares:
                    middlewares["input_queue"].send(eof_dto.to_bytes_fast())
                    logger.info(f"EOF:{new_counter} reenviado a input queue")
            else:
                # Último nodo, enviar EOF final al agregador
                eof_dto = TransactionBatchDTO("EOF:1", BatchType.EOF)
                middlewares["output"].send(eof_dto.to_bytes_fast(), 'aggregated.eof')
                logger.info("EOF final enviado a TopK intermedios (último nodo)")
            
            return True
            
        except Exception as e:
            logger.error(f"Error manejando EOF: {e}")
            return False
    
    def _send_data_by_store(self, output_middleware, strategy):
        """Envía los datos acumulados por la strategy, agrupados por store_id"""
        store_user_purchases = strategy.store_user_purchases
        
        if not store_user_purchases:
            logger.warning("No hay datos locales para enviar")
            return
        
        total_stores = len(store_user_purchases)
        logger.info(f"Enviando datos de {total_stores} stores")
        
        for store_id in sorted(store_user_purchases.keys()):
            store_csv_lines = ["store_id,user_id,purchases_qty"]
            user_purchases = store_user_purchases[store_id]
            
            for user_purchase in user_purchases.values():
                store_csv_lines.append(user_purchase.to_csv_line(store_id))
            
            store_csv = '\n'.join(store_csv_lines)
            routing_key = f"store.{store_id}"
            
            result_dto = TransactionBatchDTO(store_csv, BatchType.RAW_CSV)
            output_middleware.send(result_dto.to_bytes_fast(), routing_key)
            
            logger.info(f"Store {store_id}: {len(store_csv_lines)-1} users → '{routing_key}'")
    
    def get_strategy_config(self) -> dict:
        return {
            'input_queue_name': self.input_queue_name
        }

