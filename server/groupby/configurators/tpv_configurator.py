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
        self.eof_received_by_client: Dict[str, bool] = {}

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
    
    def process_message(self, body: bytes, headers: dict = None) -> tuple:
        dto = TransactionBatchDTO.from_bytes_fast(body)
        
        client_id = 'default_client'
        if headers and 'client_id' in headers:
            client_id = headers['client_id']
            if isinstance(client_id, bytes):
                client_id = client_id.decode('utf-8')
        
        dto.client_id = client_id
        
        logger.info(f"Mensaje recibido de cliente '{client_id}': batch_type={dto.batch_type}, tamaño={len(dto.data)} bytes")
        
        is_eof = (dto.batch_type == BatchType.EOF)
        should_stop = False
        
        return (should_stop, dto, is_eof)
    
    def handle_eof(self, dto: TransactionBatchDTO, middlewares: dict, strategy) -> bool:
        client_id = getattr(dto, 'client_id', 'default_client')
        
        if self.eof_received_by_client.get(client_id, False):
            logger.warning(f"EOF duplicado de cliente '{client_id}', ignorando")
            return False
        
        logger.info(f"EOF recibido de cliente '{client_id}'")
        self.eof_received_by_client[client_id] = True
        
        self._send_results_for_client(client_id, middlewares, strategy)
        
        return False  
    
    def _send_results_for_client(self, client_id: str, middlewares: dict, strategy):
        logger.info(f"Generando resultados TPV para cliente '{client_id}'")
        
        # Obtener resultados de la strategy para este cliente
        results_csv = strategy.generate_results_csv_for_client(client_id)
        
        result_dto = TransactionBatchDTO(results_csv, BatchType.RAW_CSV)
        middlewares["output"].send(
            result_dto.to_bytes_fast(),
            routing_key='tpv.data',
            headers={'client_id': client_id}
        )
        
        eof_dto = TransactionBatchDTO(f"EOF:{client_id}", BatchType.EOF)
        middlewares["output"].send(
            eof_dto.to_bytes_fast(),
            routing_key='tpv.eof',
            headers={'client_id': client_id}
        )
        
        logger.info(f"Resultados TPV enviados para cliente '{client_id}'")
    
    def get_strategy_config(self) -> dict:
        return {
            'semester': self.semester
        }
