import logging
from typing import Optional, Dict, Any
from rabbitmq.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
from dtos.dto import TransactionBatchDTO, BatchType
from .base_configurator import NodeConfigurator

logger = logging.getLogger(__name__)


class AmountNodeConfigurator(NodeConfigurator):
    
    
    def create_input_middleware(self, input_queue: str, node_id: str):
        logger.info(f"AmountNode: Usando working queue compartida '{input_queue}'")
        
        return MessageMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=input_queue
        )
    
    def create_output_middlewares(self, output_q1: Optional[str], output_q3: Optional[str],
                                  output_q4: Optional[str] = None, output_q2: Optional[str] = None) -> Dict[str, Any]:
        middlewares = {}
        logger.info(f"Configurando middlewares de salida para AmountNodeConfigurator {output_q1}")
        if output_q1:
            middlewares['q1'] = MessageMiddlewareExchange(
                host=self.rabbitmq_host,
                exchange_name=output_q1,
                route_keys=['q1.data']
            )
            logger.info(f"  Output Q1 Exchange: {output_q1}")
        
        return middlewares
    
    def process_filtered_data(self, filtered_csv: str) -> str:
        return self._extract_q1_columns(filtered_csv)
    
    def send_data(self, data: str, middlewares: Dict[str, Any], batch_type: str = "transactions",client_id: Optional[int] = None):
        headers = self.create_headers(client_id)
        if 'q1' in middlewares:
            filtered_dto = TransactionBatchDTO(data, batch_type=BatchType.RAW_CSV)
            middlewares['q1'].send(
                filtered_dto.to_bytes_fast(),
                routing_key='q1.data',
                headers=headers
            )
            logger.info(f"Datos enviados a Q1 exchange con routing key 'q1.data'")

    def send_eof(self, middlewares: Dict[str, Any], batch_type: str = "transactions", client_id: Optional[int] = None):
        if 'q1' in middlewares:
            eof_dto = TransactionBatchDTO("EOF:1", BatchType.EOF)
            middlewares['q1'].send(
                eof_dto.to_bytes_fast(),
                routing_key='q1.data',
                headers=self.create_headers(client_id)
            )
            logger.info("EOF:1 enviado a Q1 exchange con routing key 'q1.data'")

    def _extract_q1_columns(self, csv_data: str) -> str:
        result_lines = ["transaction_id,final_amount"]
        
        for line in csv_data.split('\n'):
            line = line.strip()
            if not line:
                continue
            
            parts = line.split(',')
            if len(parts) >= 8:
                transaction_id = parts[0]
                final_amount = parts[7]
                result_lines.append(f"{transaction_id},{final_amount}")
        
        logger.info(f"Extraídas {len(result_lines)-1} líneas con columnas Q1")
        return '\n'.join(result_lines)
    
    

    def handle_eof(self, counter: int, total_filters: int, eof_type: str,
                   middlewares: Dict[str, Any], input_middleware: Any, client_id: Optional[int] = None) -> bool:
        logger.info(f"AmountNode: EOF counter={counter}, total_filters={total_filters}")
        
        if counter < total_filters:
            self._forward_eof_to_input(counter + 1, eof_type, input_middleware, client_id=client_id)
            #return False
        
        elif counter == total_filters:
            logger.info(f"AmountNode: EOF llegó al último filtro - enviando y cerrando")
            self.send_eof(middlewares, "transactions", client_id=client_id)
            #return True  
        
        return True

    def _forward_eof_to_input(self, new_counter: int, eof_type: str, input_middleware: Any, client_id: Optional[int] = None):
        eof_message = f"EOF:{new_counter}"

        headers = self.create_headers(client_id)
        eof_dto = TransactionBatchDTO(eof_message, batch_type=BatchType.EOF)

        input_middleware.send(eof_dto.to_bytes_fast(), headers=headers)
        #input_middleware.close()

        logger.info(f"AmountNode: {eof_message} reenviado")
        
    def process_message(self, body: bytes, routing_key: str = None) -> tuple:
        decoded_data = body.decode('utf-8').strip()
        
        if decoded_data.startswith("EOF:"):
            dto = TransactionBatchDTO.from_bytes_fast(body)
            return (None, 'transactions', dto, True)
        
        dto = TransactionBatchDTO(decoded_data, BatchType.RAW_CSV)
        return (False, 'transactions', dto, False)