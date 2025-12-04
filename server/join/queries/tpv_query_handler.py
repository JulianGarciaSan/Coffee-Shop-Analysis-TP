
import logging
import time
from typing import Dict, List

from dtos.dto import BatchType, TransactionBatchDTO

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TPVQueryHandler:
    def __init__(self, output_middleware, join_node):
        self.output_middleware = output_middleware
        self.join_node = join_node

    def send_q3_results(self, client_id: str, joined_data: List[Dict]):
        try:
            if not joined_data:
                logger.warning(f"No hay datos para Q3 de cliente '{client_id}'")
                return
            self.join_node.checkpoint_handler.start_batch_transaction(client_id, "q3")                
            self.send_data(client_id, joined_data)
            self.send_eof(client_id)
            self.join_node.checkpoint_handler.commit_batch_transaction(client_id, "q3")

        except Exception as e:
            logger.error(f"Error enviando resultados Q3 para cliente '{client_id}': {e}", exc_info=True)
            
    def send_data(self, client_id: str, joined_data: List[Dict]):
        sorted_data = sorted(joined_data, key=lambda x: (x['year_half_created_at'], int(x['store_id'])))
        header = "year_half_created_at,store_name,tpv"
        csv_lines = [header]

        for record in sorted_data:
            store_name = str(record['store_name']).replace(',', '_')
            csv_lines.append(f"{record['year_half_created_at']},{store_name},{record['tpv']:.1f}")
        
        results_csv = '\n'.join(csv_lines)
        unique_id = self.join_node.checkpoint_handler.get_next_id_in_memory(client_id)
        result_dto = TransactionBatchDTO(results_csv, BatchType.RAW_CSV)
        self.output_middleware.send(
            result_dto.to_bytes_fast(), 
            routing_key=f'q3.data',
            headers={'client_id': int(client_id), 'message_id': unique_id}
        )
        print(f"///////////// ENVIANDO QUERY Q3 MESSAGE ID: {unique_id} /////////////")
        # time.sleep(10)
    def send_eof(self, client_id: str):    
        eof_dto = TransactionBatchDTO(f"EOF:{client_id}", BatchType.EOF)
        unique_id = self.join_node.checkpoint_handler.get_next_id_in_memory(client_id)
        self.output_middleware.send(
            eof_dto.to_bytes_fast(), 
            routing_key=f'q3.data',
            headers={'client_id': int(client_id), 'message_id': unique_id}
        )
        print(f"///////////// ENVIANDO EOF Q3 MESSAGE ID: {unique_id} /////////////")
        # time.sleep(10)
    def _parse_tpv_line(self, line: str) -> Dict:
        if line.startswith('year_half_created_at'):
            return None
        parts = line.split(',')
        if len(parts) >= 4:
            return {
                'year_half_created_at': parts[0],
                'store_id': parts[1],
                'total_payment_value': float(parts[2]),
                'transaction_count': int(parts[3])
            }
        return None
    
    def handle_tpv_message(self, message: bytes, client_id : str, message_id : str) -> bool:
        self.join_node._get_or_create_processors(client_id)
        
        dto = TransactionBatchDTO.from_bytes_fast(message)
        
        if dto.batch_type == BatchType.RAW_CSV:
            lines = dto.data.split('\n')
            csv_lines_with_prefix = []
            
            for line in lines:
                if line.strip():
                    if line.strip() == 'year_half_created_at,store_id,total_payment_value,transaction_count':
                        continue
                    csv_lines_with_prefix.append(f"tpv:{line.strip()}")
            
            self.join_node.tpv_processors[client_id].process_batch(dto.data, self._parse_tpv_line)
            self.join_node.checkpoint_handler.save_message_checkpoint(
                client_id, message_id, csv_lines_with_prefix
            )
            return (False, True)
        
        if dto.batch_type == BatchType.EOF:
            if dto.data.startswith("EOF:2") or dto.data.startswith("EOF:3"):
                eof_type = 2 if dto.data.startswith("EOF:2") else 3
                client_id = str(client_id)
                logger.info(f"EOF tipo {eof_type} recibido para '{client_id}'")
                if eof_type == 2:
                    self.join_node.clean_client_data(client_id)
                else:
                    self.join_node.clean_all_data()
                return (False, True)
            
            self.join_node.client_states[client_id].groupby_eof_count += 1
            logger.info(f"EOF TPV para '{client_id}': {self.join_node.client_states[client_id].groupby_eof_count}/{self.join_node.client_states[client_id].expected_groupby_nodes}")
            self.join_node._check_and_execute_joins(client_id)
            lines = [f"EOF:tpv"]
            self.join_node.checkpoint_handler.save_message_checkpoint(
                client_id, message_id, lines
            )
            return (False, True)
        
        return (False, False)
    
