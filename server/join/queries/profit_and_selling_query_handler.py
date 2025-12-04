from collections import defaultdict
import logging
import time
from typing import Dict, List

from dtos.dto import BatchType, TransactionBatchDTO, TransactionItemBatchDTO
from client_processing_state import ClientProcessingState

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ProfitAndSellingQueryHandler:
    def __init__(self, output_middleware, join_node):
        self.output_middleware = output_middleware
        self.join_node = join_node


    def send_best_selling_results(self, client_id: str, joined_data: List[Dict]):
        try:
            if not joined_data:
                logger.warning(f"No hay datos para Q2 best_selling de cliente '{client_id}'")
                return
            self.join_node.checkpoint_handler.start_batch_transaction(client_id, "q2_best_selling")                
            self.send_best_selling_data(client_id, joined_data)
            self.send_eof(client_id,'q2_best_selling.data')
            self.join_node.checkpoint_handler.commit_batch_transaction(client_id, "q2_best_selling")

        except Exception as e:
            logger.error(f"Error enviando resultados Q2 best_selling para cliente '{client_id}': {e}", exc_info=True)
    
    def send_most_profit_results(self, client_id: str, joined_data: List[Dict]):
        try:
            if not joined_data:
                logger.warning(f"No hay datos para Q2 most_profit de cliente '{client_id}'")
                return
            self.join_node.checkpoint_handler.start_batch_transaction(client_id, "q2_most_profit")                

            self.send_most_profit_data(client_id, joined_data)
            self.send_eof(client_id,'q2_most_profit.data')
            self.join_node.checkpoint_handler.commit_batch_transaction(client_id, "q2_most_profit")

            logger.info(f"Resultados Q2 most_profit completados para cliente '{client_id}': {len(joined_data)} registros en total")

        except Exception as e:
            logger.error(f"Error enviando resultados Q2 most_profit para cliente '{client_id}': {e}", exc_info=True)     
    
    def send_most_profit_data(self, client_id: str, joined_data: List[Dict]):
        sorted_data = sorted(joined_data, key=lambda x: x['year_month_created_at'])

        header = "year_month_created_at,item_name,profit_sum"

        csv_lines = [header]

        for record in sorted_data:
            csv_lines.append(f"{record['year_month_created_at']},{record['item_name']},{record['profit_sum']:.1f}")
        
        results_csv = '\n'.join(csv_lines)
        
        result_dto = TransactionItemBatchDTO(results_csv, BatchType.RAW_CSV)
        unique_id = self.join_node.checkpoint_handler.get_next_id_in_memory(client_id)

        self.output_middleware.send(
            result_dto.to_bytes_fast(), 
            routing_key='q2_most_profit.data',
            headers={'client_id': int(client_id), 'message_id': unique_id}
        )
        print(f"///////////// ENVIANDO MESSAGE Q2 MP MESSAGE ID: {unique_id} /////////////")
        # time.sleep(10)
    
    def send_best_selling_data(self, client_id: str, joined_data: List[Dict]):
        sorted_data = sorted(joined_data, key=lambda x: x['year_month_created_at'])
        header = "year_month_created_at,item_name,sellings_qty"
        csv_lines = [header]

        for record in sorted_data:
            csv_lines.append(f"{record['year_month_created_at']},{record['item_name']},{record['sellings_qty']}")
        
        results_csv = '\n'.join(csv_lines)
        unique_id = self.join_node.checkpoint_handler.get_next_id_in_memory(client_id)

        result_dto = TransactionItemBatchDTO(results_csv, BatchType.RAW_CSV)
        self.output_middleware.send(
            result_dto.to_bytes_fast(), 
            routing_key='q2_best_selling.data',
            headers={'client_id': int(client_id), 'message_id': unique_id}
            )
        print(f"///////////// ENVIANDO QUERY Q2 BS MESSAGE ID: {unique_id} /////////////")
        # time.sleep(10)
    def send_eof(self, client_id: str, routing_key: str):
        eof_dto = TransactionItemBatchDTO(f"EOF:{client_id}", BatchType.EOF)
        unique_id = self.join_node.checkpoint_handler.get_next_id_in_memory(client_id)

        self.output_middleware.send(
            eof_dto.to_bytes_fast(), 
            routing_key=routing_key,
            headers={'client_id': int(client_id), 'message_id': unique_id}
        )
        print(f"///////////// ENVIANDO EOF Q2 {routing_key} MESSAGE ID: {unique_id} /////////////")    
        # time.sleep(10)
    
    def _parse_best_selling_line(self, line: str) -> Dict:
        if line.startswith('created_at'):
            return None
        parts = line.split(',')
        if len(parts) >= 3:
            return {
                'year_month_created_at': parts[0],
                'item_id': parts[1],
                'sellings_qty': int(parts[2])
            }
        return None
    
    def _parse_most_profit_line(self, line: str) -> Dict:
        if line.startswith('created_at'):
            return None
        parts = line.split(',')
        if len(parts) >= 3:
            return {
                'year_month_created_at': parts[0],
                'item_id': parts[1],
                'profit_sum': float(parts[2])
            }
        return None
    
    
    def handle_best_selling_message(self, message: bytes, client_id, message_id) -> bool:
        self.join_node._get_or_create_processors(client_id)
        
        dto = TransactionItemBatchDTO.from_bytes_fast(message)
        
        if dto.batch_type == BatchType.RAW_CSV:
            lines = dto.data.split('\n')
            csv_lines_with_prefix = []
            
            for line in lines:
                if line.strip():
                    if line.strip() == 'created_at,item_id,sellings_qty':
                        continue
                    csv_lines_with_prefix.append(f"best_selling:{line.strip()}")
            
            self.join_node.best_selling_processors[client_id].process_batch(dto.data, self._parse_best_selling_line)
                        
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
            if client_id not in self.join_node.client_states:
                self.join_node.client_states[client_id] = ClientProcessingState()
            
            self.join_node.client_states[client_id].best_selling_loaded = True
            logger.info(f"EOF best_selling para '{client_id}'")
            self.join_node._check_and_execute_joins(client_id)
            lines = [f"EOF:best_selling"]
            self.join_node.checkpoint_handler.save_message_checkpoint(
                client_id, message_id, lines
            )
            return (False, True)
        
        return (False, False)

    def handle_most_profit_message(self, message: bytes, client_id, message_id) -> bool:
        self.join_node._get_or_create_processors(client_id)
        
        dto = TransactionItemBatchDTO.from_bytes_fast(message)
        
        if dto.batch_type == BatchType.RAW_CSV:
            lines = dto.data.split('\n')
            csv_lines_with_prefix = []
            
            for line in lines:
                if line.strip():
                    if line.strip() == 'created_at,item_id,profit_sum':
                        continue
                    csv_lines_with_prefix.append(f"most_profit:{line.strip()}")
            
            self.join_node.most_profit_processors[client_id].process_batch(dto.data, self._parse_most_profit_line)
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
            
            self.join_node.client_states[client_id].most_profit_loaded = True
            logger.info(f"EOF most_profit para '{client_id}'")
            self.join_node._check_and_execute_joins(client_id)
            lines = [f"EOF:most_profit"]
            self.join_node.checkpoint_handler.save_message_checkpoint(
                client_id, message_id, lines
            )
            return (False, True)
        
        return (False, False)
    