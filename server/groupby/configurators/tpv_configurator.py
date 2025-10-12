from collections import defaultdict
import logging
import os
from typing import Dict, Any, Tuple
from rabbitmq.middleware import MessageMiddlewareExchange
from dtos.dto import TransactionBatchDTO, BatchType
from .base_configurators import GroupByConfigurator

logger = logging.getLogger(__name__)


class TPVConfigurator(GroupByConfigurator):
    def __init__(self, rabbitmq_host: str, output_exchange: str):
        super().__init__(rabbitmq_host, output_exchange)
        self.semester = os.getenv('SEMESTER', '1')

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
    
    def handle_eof(self, dto: TransactionBatchDTO, middlewares: dict, strategy) -> bool:
        logger.info("EOF recibido. Obteniendo resultados TPV de la strategy")
        
        results_csv = strategy.generate_results_csv()
        
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
    
    def get_strategy_config(self) -> dict:
        return {
            'semester': self.semester
        }

