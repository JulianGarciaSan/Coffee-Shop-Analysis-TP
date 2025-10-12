import logging
import os
from typing import Dict, Any
from rabbitmq.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
from dtos.dto import TransactionItemBatchDTO, BatchType
from .base_configurators import GroupByConfigurator

logger = logging.getLogger(__name__)


class BestSellingConfigurator(GroupByConfigurator):
    def __init__(self, rabbitmq_host: str, output_exchange: str):
        super().__init__(rabbitmq_host, output_exchange)
        self.year = os.getenv('AGGREGATOR_YEAR', '2024')
        self.input_exchange = os.getenv('INPUT_EXCHANGE', 'year_filtered_q2')
        self.input_queue_name = f"best_selling_{self.year}_queue"
        self.total_groupby_nodes = int(os.getenv('TOTAL_GROUPBY_NODES', '4'))
        
        # Override dto_helper para usar TransactionItemBatchDTO
        self.dto_helper = TransactionItemBatchDTO("", BatchType.RAW_CSV)
        
        logger.info(f"BestSellingConfigurator inicializado:")
        logger.info(f"  Year: {self.year}")
        logger.info(f"  Input Exchange: {self.input_exchange}")
        logger.info(f"  Input Queue: {self.input_queue_name}")
        logger.info(f"  Total nodos: {self.total_groupby_nodes}")
    
    def create_input_middleware(self):
        middleware = MessageMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.input_queue_name,
            exchange_name=self.input_exchange,
            routing_keys=[self.year]
        )
        
        logger.info(f"  Input: Queue {self.input_queue_name} bindeada a exchange {self.input_exchange} con routing key {self.year}")
        return middleware
    
    def create_output_middlewares(self) -> Dict[str, Any]:
        output_middleware = MessageMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name=self.output_exchange,
            route_keys=['2024', '2025']
        )
        
        logger.info(f"  Output exchange: {self.output_exchange}")
        logger.info(f"  Routing keys: 2024, 2025")
        
        return {"output": output_middleware}
    
    def process_message(self, body: bytes, headers: dict = None) -> tuple:
        dto = TransactionItemBatchDTO.from_bytes_fast(body)
        
        logger.info(f"Mensaje recibido: batch_type={dto.batch_type}, tamaño={len(dto.data)} bytes")
        
        is_eof = (dto.batch_type == BatchType.EOF)
        should_stop = False
        
        return (should_stop, dto, is_eof)
    
    def handle_eof(self, dto: TransactionItemBatchDTO, middlewares: dict, strategy) -> bool:
        try:
            eof_data = dto.data.strip()
            counter = int(eof_data.split(':')[1]) if ':' in eof_data else 1
            
            logger.info(f"EOF recibido con counter={counter}, total={self.total_groupby_nodes}")
            
            self._send_data_by_month(middlewares["output"], strategy)
            
            if counter < self.total_groupby_nodes:
                new_counter = counter + 1
                eof_dto = TransactionItemBatchDTO(f"EOF:{new_counter}", BatchType.EOF)
                
                if 'input_queue' in middlewares:
                    middlewares["input_queue"].send(eof_dto.to_bytes_fast())
                    logger.info(f"EOF:{new_counter} reenviado a input queue")
                    logger.info("Datos enviados - esperando otros nodos")
            else:
                # Último nodo, enviar EOF final al agregator
                logger.info(f"Último nodo GroupBy del año {self.year} - iniciando EOF para aggregators")
                eof_dto = TransactionItemBatchDTO("EOF:1", BatchType.EOF)
                
                middlewares["output"].send(eof_dto.to_bytes_fast(), routing_key=self.year)
                logger.info(f"EOF:1 enviado al topic '{self.year}' para aggregators intermediate")
                print(f"{'='*60}\n")
            
            return True
            
        except Exception as e:
            logger.error(f"Error manejando EOF: {e}")
            return False
    
    def _send_data_by_month(self, output_middleware, strategy):
        """Envía los datos acumulados por la strategy, agrupados por mes"""
        month_item_aggregations = strategy.month_item_aggregations
        
        if not month_item_aggregations:
            logger.warning("No hay datos locales para enviar")
            return
        
        total_months = len(month_item_aggregations)
        logger.info(f"Enviando datos de {total_months} meses")
        
        for year_month in sorted(month_item_aggregations.keys()):
            month_csv_lines = ["created_at,item_id,sellings_qty,profit_sum"]
            item_aggs = month_item_aggregations[year_month]
            
            for item_agg in item_aggs.values():
                if item_agg is not None:
                    month_csv_lines.append(item_agg.to_csv_line(year_month))
            
            month_csv = '\n'.join(month_csv_lines)
            
            result_dto = TransactionItemBatchDTO(month_csv, BatchType.RAW_CSV)
            output_middleware.send(result_dto.to_bytes_fast(), self.year)
            
            logger.info(f"Month {year_month}: {len(month_csv_lines)-1} items → año '{self.year}'")
    
    def get_strategy_config(self) -> dict:
        return {
            'input_queue_name': self.input_queue_name,
            'year': self.year
        }
