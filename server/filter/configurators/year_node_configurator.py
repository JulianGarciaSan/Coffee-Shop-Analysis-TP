import logging
import os
from typing import Optional, Dict, Any
from rabbitmq.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from dtos.dto import TransactionBatchDTO, TransactionItemBatchDTO, BatchType
from .base_configurator import NodeConfigurator

logger = logging.getLogger(__name__)


class YearNodeConfigurator(NodeConfigurator):
    def __init__(self, rabbitmq_host: str):
        super().__init__(rabbitmq_host)
        self.eof_received_transactions = False
        self.eof_received_transaction_items = False
        self.file_mode = os.getenv('FILE_MODE', 'transactions')  
    
    def create_input_middleware(self, input_exchange: str, input_queue: str):
        
        if self.file_mode == 'transactions':
            queue_name = 'year_transactions_queue'
            routing_key = 'transactions'
        else:  
            queue_name = 'year_items_queue'
            routing_key = 'transaction_items'
        
        logger.info(f"YearNode: Queue={queue_name}, routing_key={routing_key}")
        
        return MessageMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=queue_name,
            exchange_name=input_exchange,
            routing_keys=[routing_key]
        )
    
    def process_message(self, body: bytes, routing_key: str = None) -> tuple:
        decoded_data = body.decode('utf-8').strip()
        
        if decoded_data.startswith("EOF:"):
            if self.file_mode == 'transactions':
                dto = TransactionBatchDTO.from_bytes_fast(body)
                return (None, 'transactions', dto, True)
            else:
                dto = TransactionItemBatchDTO.from_bytes_fast(body)
                return (None, 'transaction_items', dto, True)
        
        if self.file_mode == 'transactions':
            logger.debug(f"YearNode: Mensaje recibido para 'transactions', tamaño {len(decoded_data)} bytes")
            dto = TransactionBatchDTO(decoded_data, BatchType.RAW_CSV)
            return (False, 'transactions', dto, False)
        else:
            dto = TransactionItemBatchDTO(decoded_data, BatchType.RAW_CSV)
            return (False, 'transaction_items', dto, False)
    
    def create_output_middlewares(self, output_q1: Optional[str], output_q3: Optional[str],
                                  output_q4: Optional[str] = None, output_q2: Optional[str] = None) -> Dict[str, Any]:
        middlewares = {}

        if output_q1:
            middlewares['q1'] = MessageMiddlewareQueue(
                host=self.rabbitmq_host,
                queue_name=output_q1
            )
            logger.info(f"  Output Q1 Queue: {output_q1}")
        
        if output_q2:
            middlewares['q2'] = MessageMiddlewareExchange(
                host=self.rabbitmq_host,
                exchange_name=output_q2,
                route_keys=['2024', '2025']
            )
            logger.info(f"  Output Q2 Exchange: {output_q2}")
        
        if output_q4:
            middlewares['q4'] = MessageMiddlewareQueue(
                host=self.rabbitmq_host,
                queue_name=output_q4
            )
            logger.info(f"  Output Q4 Queue: {output_q4}")
        
        return middlewares
    
    def process_filtered_data(self, filtered_csv: str) -> str:
        return filtered_csv
    
    def send_data(self, data: str, middlewares: Dict[str, Any], batch_type: str = "transactions", client_id: Optional[int] = None):
        headers = self.create_headers(client_id)
        if batch_type == "transactions":
            filtered_dto = TransactionBatchDTO(data, batch_type=BatchType.RAW_CSV)
            
            if 'q1' in middlewares:
                middlewares['q1'].send(filtered_dto.to_bytes_fast(), headers=headers)
            
            if 'q4' in middlewares:
                middlewares['q4'].send(filtered_dto.to_bytes_fast())
        
        elif batch_type == "transaction_items":
            logger.info(f"Procesando líneas de TransactionItems para Q2")
            if 'q2' in middlewares:
                self._send_transaction_items_by_year(data, middlewares['q2'])

    def send_eof(self, middlewares: Dict[str, Any], batch_type: str = "transactions", client_id: Optional[int] = None):
        headers = self.create_headers(client_id)
        if batch_type == "transactions":
            eof_dto = TransactionBatchDTO("EOF:1", batch_type=BatchType.EOF)
            
            if 'q1' in middlewares:
                middlewares['q1'].send(eof_dto.to_bytes_fast(), headers=headers)
                logger.info("EOF:1 (TransactionBatch) enviado a Q1 queue")
            
            if 'q4' in middlewares:
                middlewares['q4'].send(eof_dto.to_bytes_fast())
                logger.info("EOF:1 (TransactionBatch) enviado a Q4 queue")
        
        elif batch_type == "transaction_items":
            if 'q2' in middlewares:
                eof_dto = TransactionItemBatchDTO("EOF:1", batch_type=BatchType.EOF)
                middlewares['q2'].send(eof_dto.to_bytes_fast(), routing_key='2024')
                middlewares['q2'].send(eof_dto.to_bytes_fast(), routing_key='2025')
                logger.info("EOF:1 (TransactionItemBatch) enviado a Q2 exchange con routing keys 2024 y 2025")
    
    def handle_eof(self, counter: int, total_filters: int, eof_type: str, 
            middlewares: Dict[str, Any], input_middleware: Any, client_id: Optional[int] = None) -> bool:
        logger.info(f"YearNode: EOF counter={counter}/{total_filters}")
        
        if counter < total_filters:
            self._forward_eof(counter + 1, eof_type, input_middleware, client_id=client_id)

        elif counter == total_filters:
            logger.info(f"YearNode: EOF llegó al último - enviando downstream y cerrando")
            
            if self.file_mode == 'transactions':
                self.send_eof(middlewares, "transactions",client_id=client_id)
            else:
                self.send_eof(middlewares, "transaction_items")
            
        
        return True

    def _forward_eof(self, new_counter: int, eof_type: str, input_middleware: Any, client_id: Optional[int] = None):
        headers = self.create_headers(client_id)
        eof_message = f"EOF:{new_counter}" 
        
        if self.file_mode == 'transactions':
            eof_dto = TransactionBatchDTO(eof_message, batch_type=BatchType.EOF)
        else:
            eof_dto = TransactionItemBatchDTO(eof_message, batch_type=BatchType.EOF)
        
        input_middleware.send(eof_dto.to_bytes_fast(), headers=headers)
        logger.info(f"YearNode: {eof_message} reenviado")
    
    def _send_transaction_items_by_year(self, data: str, q2_middleware):
        lines = data.strip().split('\n')
        header = lines[0] if lines else ""
        
        logger.info(f"Procesando {len(lines)} líneas de TransactionItems para Q2")
        
        data_by_year = {'2024': [header], '2025': [header]}
        dto_helper = TransactionItemBatchDTO("", BatchType.RAW_CSV)
        
        for line in lines[1:]:
            if not line.strip():
                continue
                
            try:
                created_at = dto_helper.get_column_value(line, 'created_at')
                if created_at and len(created_at) >= 4:
                    year = created_at[:4]
                    if year in data_by_year:
                        data_by_year[year].append(line)
                else:
                    logger.warning(f"Created_at inválido: '{created_at}' en línea: {line[:50]}...")
            except Exception as e:
                logger.warning(f"Error procesando línea TransactionItem para Q2: {e}")
                continue
        
        for year, year_lines in data_by_year.items():
            if len(year_lines) > 1:
                year_csv = '\n'.join(year_lines)
                year_dto = TransactionItemBatchDTO(year_csv, batch_type=BatchType.RAW_CSV)
                q2_middleware.send(year_dto.to_bytes_fast(), routing_key=year)
                logger.info(f"TransactionItemBatchDTO enviado a Q2 con routing key {year}: {len(year_lines)-1} líneas")
            else:
                logger.info(f"No hay datos para año {year} - solo header")