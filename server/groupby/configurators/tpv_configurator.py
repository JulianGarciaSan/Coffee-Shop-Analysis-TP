from collections import defaultdict
import logging
import os
from typing import Dict, Any, Tuple
from rabbitmq.middleware import MessageMiddlewareExchange
from dtos.dto import TransactionBatchDTO, BatchType
from .base_configurators import GroupByConfigurator
from .tpv_aggregation import TPVAggregation

logger = logging.getLogger(__name__)


class TPVConfigurator(GroupByConfigurator):
    def __init__(self, rabbitmq_host: str, output_exchange: str):
        super().__init__(rabbitmq_host, output_exchange)
        self.semester = os.getenv('SEMESTER', '1')
        self.tpv_aggregations: Dict[Tuple[str, str], TPVAggregation] = defaultdict(TPVAggregation)
        logger.info(f"TPVGroupByStrategy inicializada para semestre {self.semester}")

        if self.semester not in ['1', '2']:
            raise ValueError("SEMESTER debe ser '1' o '2'")
        
        self.input_exchange = os.getenv('INPUT_EXCHANGE', 'groupby.join.exchange')
        
        logger.info(f"TPVConfigurator inicializado:")
        logger.info(f"  Semestre: {self.semester}")
        logger.info(f"  Input Exchange: {self.input_exchange}")
    
    def create_input_middleware(self):
        middleware = MessageMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name=self.input_exchange,
            route_keys=[f'semester.{self.semester}', 'eof.all']
        )
        
        logger.info(f"  Input: Exchange {self.input_exchange}, semestre {self.semester}")
        return middleware
    
    def create_output_middlewares(self) -> Dict[str, Any]:
        output_middleware = MessageMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name=self.output_exchange,
            route_keys=['tpv.data']
        )
        
        logger.info(f"  Output exchange: {self.output_exchange}")
        logger.info(f"  Routing keys: tpv.data")
        
        return {"output": output_middleware}
    
    def process_message(self, body: bytes) -> tuple:
        dto = TransactionBatchDTO.from_bytes_fast(body)
        
        logger.info(f"Mensaje recibido: batch_type={dto.batch_type}, tamaño={len(dto.data)} bytes")
        
        is_eof = (dto.batch_type == BatchType.EOF)
        should_stop = False
        
        return (should_stop, dto, is_eof)
    
    def handle_eof(self, dto: TransactionBatchDTO, middlewares: dict) -> bool:
        logger.info("EOF recibido. Generando resultados TPV finales")
        
        results_csv = self.generate_results_csv()
        
        result_dto = TransactionBatchDTO(results_csv, BatchType.RAW_CSV)
        middlewares["output"].send(
            result_dto.to_bytes_fast(),
            routing_key='tpv.data'
        )
        
        eof_dto = TransactionBatchDTO("EOF:1", BatchType.EOF)
        middlewares["output"].send(
            eof_dto.to_bytes_fast(),
            routing_key='tpv.data'
        )
        
        logger.info("Resultados TPV enviados al siguiente nodo")
        return True
    
    def generate_results_csv(self) -> str:
        if not self.tpv_aggregations:
            logger.warning("No hay datos TPV para generar resultados")
            return "year_half_created_at,store_id,total_payment_value,transaction_count"
        
        csv_lines = ["year_half_created_at,store_id,total_payment_value,transaction_count"]
        
        for (year_half, store_id) in sorted(self.tpv_aggregations.keys()):
            aggregation = self.tpv_aggregations[(year_half, store_id)]
            csv_lines.append(aggregation.to_csv_line(year_half, store_id))
        
        logger.info(f"Resultados TPV generados para {len(self.tpv_aggregations)} grupos")
        return '\n'.join(csv_lines)
    
    def get_strategy_config(self) -> dict:
        return {
            'semester': self.semester
        }
        
    def process_csv_line(self, csv_line: str):
        try:
            store_id = self.dto_helper.get_column_value(csv_line, 'store_id')
            created_at = self.dto_helper.get_column_value(csv_line, 'created_at')
            final_amount_str = self.dto_helper.get_column_value(csv_line, 'final_amount')
            
            if not all([store_id, created_at, final_amount_str]):
                return
            
            year = created_at[:4]
            year_half = f"{year}-H{self.semester}"
            final_amount = float(final_amount_str)
            
            key = (year_half, store_id)
            self.tpv_aggregations[key].add_transaction(final_amount)
            
        except (ValueError, IndexError) as e:
            logger.warning(f"Error procesando línea para TPV: {e}")
