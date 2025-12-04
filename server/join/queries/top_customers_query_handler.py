


import logging
import time
from typing import Dict, List

from dtos.dto import BatchType, TransactionBatchDTO

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TopCustomersQueryHandler:
    def __init__(self, output_middleware, join_node):
        self.output_middleware = output_middleware
        self.join_node = join_node
        
        
    def send_q4_results(self, client_id: str, joined_data: List[Dict]):
        try:
            if not joined_data:
                logger.warning(f"No hay datos para Q4 de cliente '{client_id}'")
                return
            self.join_node.checkpoint_handler.start_batch_transaction(client_id, "q4")                
            self.send_data(client_id, joined_data)
            self.send_eof(client_id)
            self.join_node.checkpoint_handler.commit_batch_transaction(client_id, "q4")
            logger.info(f"Resultados Q4 completados para cliente '{client_id}': {len(joined_data)} registros en total")
            
        except Exception as e:
            logger.error(f"Error enviando resultados Q4 para cliente '{client_id}': {e}", exc_info=True)

            
    def send_data(self, client_id: str, joined_data: List[Dict]):
        sorted_data = sorted(joined_data, key=lambda x: int(x['store_id']))

        header = "store_name,birthdate"
        csv_lines = [header]
            
        for record in sorted_data:
            csv_lines.append(f"{record['store_name']},{record['birthdate']}")
        results_csv = '\n'.join(csv_lines)
        unique_id = self.join_node.checkpoint_handler.get_next_id_in_memory(client_id)
        result_dto = TransactionBatchDTO(results_csv, BatchType.RAW_CSV)
        self.output_middleware.send(
            result_dto.to_bytes_fast(), 
            routing_key=f'q4.data',
            headers={'client_id': int(client_id), 'message_id': unique_id}
        )
        logger.info(f"Batch Q4 enviado para cliente '{client_id}')")
        print(f"///////////// ENVIANDO QUERY Q4 MESSAGE ID: {unique_id} /////////////")
        # time.sleep(10)
        
    def send_eof(self, client_id: str):
        eof_dto = TransactionBatchDTO(f"EOF:{client_id}", BatchType.EOF)
        unique_id = self.join_node.checkpoint_handler.get_next_id_in_memory(client_id)

        self.output_middleware.send(
            eof_dto.to_bytes_fast(), 
            routing_key=f'q4.data',
            headers={'client_id': int(client_id), 'message_id': unique_id}
        )
        print(f"///////////// ENVIANDO EOF Q4 MESSAGE ID: {unique_id} /////////////")
        # time.sleep(10)            
    
       
    def _parse_top_customers_line(self, line: str) -> Dict:
        if line.startswith('store_id,user_id,purchases_qty'):
            return None
        parts = line.split(',')
        if len(parts) >= 3:
            raw_user_id = parts[1]
            user_id = raw_user_id[:-2] if '.' in raw_user_id and raw_user_id.endswith('.0') else raw_user_id
            return {
                'store_id': parts[0],
                'user_id': user_id,
                'purchases_qty': int(parts[2])
            }
        return None
    
    
    def handle_top_customers_message(self, message: bytes, client_id, message_id) -> bool:
        self.join_node._get_or_create_processors(client_id)
        
        dto = TransactionBatchDTO.from_bytes_fast(message)
        
        if dto.batch_type == BatchType.RAW_CSV:
            lines = dto.data.split('\n')
            csv_lines_with_prefix = []
            
            for line in lines:
                if line.strip():
                    if line.strip() == 'store_id,user_id,purchases_qty':
                        continue
                    csv_lines_with_prefix.append(f"top_customers:{line.strip()}")
            
            
            self.join_node.top_customers_processors[client_id].process_batch(dto.data, self._parse_top_customers_line)
            self.join_node.checkpoint_handler.save_message_checkpoint(
                client_id, message_id, csv_lines_with_prefix
            )
            return (False, True)
        
        if dto.batch_type == BatchType.EOF:
            if dto.data.startswith("EOF:2"):
                client_id = str(client_id)
                logger.info(f"EOF tipo 2 recibido para '{client_id}'")
                self.join_node.clean_client_data(client_id)
                return (False, True)
            
            self.join_node.client_states[client_id].top_customers_eof_count += 1
            
            if self.join_node.client_states[client_id].top_customers_eof_count >= \
            self.join_node.client_states[client_id].expected_top_customers_aggregators:
                self.join_node.client_states[client_id].top_customers_loaded = True
                logger.info(f"Todos los EOF top_customers para '{client_id}'")
                self.join_node._check_and_execute_joins(client_id)
            lines = [f"EOF:top_customers"]
            self.join_node.checkpoint_handler.save_message_checkpoint(
                client_id, message_id, lines
            )
            return (False, True)
        
        return (False, False)
    
