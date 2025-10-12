import logging
import os
from collections import defaultdict
from typing import Dict
from .base_strategy import GroupByStrategy
from dtos.dto import TransactionItemBatchDTO, BatchType
from rabbitmq.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue

logger = logging.getLogger(__name__)


class ItemAggregation:
    def __init__(self, item_id: str):
        self.item_id = item_id
        self.sellings_qty = 0
        self.profit_sum = 0.0
    
    def add_transaction(self, quantity: int, subtotal: float): 
        self.sellings_qty += quantity  
        self.profit_sum += subtotal
    
    def to_csv_line(self, year_month: str) -> str:
        return f"{year_month},{self.item_id},{self.sellings_qty},{self.profit_sum:.2f}"


class BestSellingGroupByStrategy(GroupByStrategy):
    def __init__(self, input_queue_name: str):
        super().__init__()  
        self.input_queue_name = input_queue_name
        self.month_item_aggregations: Dict[str, Dict[str, ItemAggregation]] = defaultdict(
            lambda: defaultdict(lambda: None)
        )
        self.total_groupby_nodes = int(os.getenv('TOTAL_GROUPBY_NODES', '4'))
                
        self.year = os.getenv('AGGREGATOR_YEAR','2024')
        self.input_exchange = os.getenv('INPUT_EXCHANGE', 'year_filtered_q2')
        self.dto_helper = TransactionItemBatchDTO("", BatchType.RAW_CSV)

        logger.info(f"BestSellingGroupByStrategy inicializada")
        logger.info(f"  Total nodos: {self.total_groupby_nodes}")
        logger.info(f"  Input queue: {self.input_queue_name}")
    
    def setup_output_middleware(self, rabbitmq_host: str, output_exchange: str):
        output_middleware = MessageMiddlewareExchange(
                host=rabbitmq_host,
                exchange_name=output_exchange,
                route_keys=['2024', '2025']
            )
        logger.info(f"  Output Q2 Exchange: {output_exchange}")

        return {"output": output_middleware}
    
    def process_csv_line(self, csv_line: str):
        try:
            item_id = self.dto_helper.get_column_value(csv_line, 'item_id')
            created_at = self.dto_helper.get_column_value(csv_line, 'created_at')
            quantity_str = self.dto_helper.get_column_value(csv_line, 'quantity') 
            subtotal_str = self.dto_helper.get_column_value(csv_line, 'subtotal')
            
            if not all([item_id, created_at, quantity_str, subtotal_str]):
                return
            
            year_month = created_at[:7]
            quantity = int(quantity_str) 
            subtotal = float(subtotal_str)
            
            if self.month_item_aggregations[year_month][item_id] is None:
                self.month_item_aggregations[year_month][item_id] = ItemAggregation(item_id)
            
            self.month_item_aggregations[year_month][item_id].add_transaction(quantity, subtotal)  
            
        except (ValueError, IndexError) as e:
            logger.warning(f"Error procesando línea: {e}")
    
    def generate_results_csv(self) -> str:
        if not self.month_item_aggregations:
            logger.warning("No hay datos locales para generar")
            return "created_at,item_id,sellings_qty,profit_sum"
        
        csv_lines = ["created_at,item_id,sellings_qty,profit_sum"]
        
        for year_month in sorted(self.month_item_aggregations.keys()):
            item_aggs = self.month_item_aggregations[year_month]
            for item_agg in item_aggs.values():
                csv_lines.append(item_agg.to_csv_line(year_month))
        
        total_records = len(csv_lines) - 1
        logger.info(f"Datos locales generados: {total_records} registros")
        return '\n'.join(csv_lines)
    
    def handle_eof_message(self, dto: TransactionItemBatchDTO, middlewares: dict) -> bool:
        try:
            eof_data = dto.data.strip()
            counter = int(eof_data.split(':')[1]) if ':' in eof_data else 1
            
            logger.info(f"EOF recibido con counter={counter}, total={self.total_groupby_nodes}")
            
            self._send_data_by_month(middlewares["output"])
            
            if counter < self.total_groupby_nodes:
                new_counter = counter + 1
                eof_dto = TransactionItemBatchDTO(f"EOF:{new_counter}", BatchType.EOF)
                middlewares["input_queue"].send(eof_dto.to_bytes_fast())
                logger.info(f"EOF:{new_counter} reenviado a input queue")
                logger.info("Datos enviados - esperando otros nodos")
                #return False  
            else:
                logger.info(f"Último nodo GroupBy del año {self.year} - iniciando EOF para aggregators")
                eof_dto = TransactionItemBatchDTO("EOF:1", BatchType.EOF)
                
                middlewares["output"].send(eof_dto.to_bytes_fast(), routing_key=self.year)
                logger.info(f"EOF:1 enviado al topic '{self.year}' para aggregators intermediate")
            
                print(f"{'='*60}\n")
                #return True
            return True
                        
        except Exception as e:
            logger.error(f"Error manejando EOF: {e}")
            return False
    
    def _send_data_by_month(self, output_middleware):
        if not self.month_item_aggregations:
            logger.warning("No hay datos locales para enviar")
            return
        
        total_months = len(self.month_item_aggregations)
        logger.info(f"Enviando datos de {total_months} meses")
        
        for year_month in sorted(self.month_item_aggregations.keys()):
            month_csv_lines = ["created_at,item_id,sellings_qty,profit_sum"]
            item_aggs = self.month_item_aggregations[year_month]
            
            for item_agg in item_aggs.values():
                month_csv_lines.append(item_agg.to_csv_line(year_month))
            
            month_csv = '\n'.join(month_csv_lines)
            
            result_dto = TransactionItemBatchDTO(month_csv, BatchType.RAW_CSV)
            output_middleware.send(result_dto.to_bytes_fast(), self.year)
            
            logger.info(f"Month {year_month}: {len(month_csv_lines)-1} items → año '{self.year}'")
    
    def cleanup_middlewares(self, middlewares: dict):
        try:
            for middleware in middlewares.values():
                if hasattr(middleware, 'close'):
                    middleware.close()
            logger.info("Output middlewares cerrados")
        except Exception as e:
            logger.error(f"Error cerrando middlewares: {e}")