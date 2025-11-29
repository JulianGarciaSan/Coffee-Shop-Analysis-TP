
import logging
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
            
            sorted_data = sorted(joined_data, 
                               key=lambda x: (x['year_half_created_at'], int(x['store_id'])))
            print(f"\n=== RESULTADOS Q3 PARA CLIENTE '{client_id}' ===")
            print("year_half_created_at,store_name,tpv")
            for record in sorted_data:
                store_name = str(record['store_name']).replace(',', '_')
                print(f"{record['year_half_created_at']},{store_name},{record['tpv']:.1f}")
            print(f"=== FIN RESULTADOS Q3 PARA CLIENTE '{client_id}' ===\n")
            
            BATCH_SIZE = 150
            header = "year_half_created_at,store_name,tpv"
            
            logger.info(f"Enviando Q3 para cliente '{client_id}': {len(sorted_data)} registros en {(len(sorted_data) + BATCH_SIZE - 1) // BATCH_SIZE} batches")
            
            for i in range(0, len(sorted_data), BATCH_SIZE):
                batch = sorted_data[i:i + BATCH_SIZE]
                csv_lines = [header]
                
                for record in batch:
                    store_name = str(record['store_name']).replace(',', '_')
                    csv_lines.append(f"{record['year_half_created_at']},{store_name},{record['tpv']:.1f}")
                
                results_csv = '\n'.join(csv_lines)
                
                logger.info(f"Tamaño del batch {i//BATCH_SIZE + 1}: {len(results_csv)} bytes")
                
                result_dto = TransactionBatchDTO(results_csv, BatchType.RAW_CSV)
                self.output_middleware.send(
                    result_dto.to_bytes_fast(), 
                    routing_key=f'client.{client_id}.q3',
                    headers={'client_id': int(client_id)}
                )
                
                logger.info(f"Batch Q3 enviado para cliente '{client_id}': {len(batch)} registros ({i+1}-{i+len(batch)}/{len(sorted_data)})")
            
            eof_dto = TransactionBatchDTO(f"EOF:{client_id}", BatchType.EOF)
            self.output_middleware.send(
                eof_dto.to_bytes_fast(), 
                routing_key=f'client.{client_id}.q3',
                headers={'client_id': int(client_id)}
            )
            
            logger.info(f"Resultados Q3 completados para cliente '{client_id}': {len(joined_data)} registros en total")
            
        except Exception as e:
            logger.error(f"Error enviando resultados Q3 para cliente '{client_id}': {e}", exc_info=True)
            
            
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
            csv_lines_with_prefix = [f"tpv:{line}" for line in dto.data.split('\n') if line.strip()]
            
            self.join_node.checkpoint_handler.save_message_checkpoint(
                client_id, message_id, csv_lines_with_prefix
            )
            
            self.join_node.tpv_processors[client_id].process_batch(dto.data, self._parse_tpv_line)
            return (False, True)
        
        if dto.batch_type == BatchType.EOF:
            self.join_node.client_states[client_id].groupby_eof_count += 1
            logger.info(f"EOF TPV para '{client_id}': {self.join_node.client_states[client_id].groupby_eof_count}/{self.join_node.client_states[client_id].expected_groupby_nodes}")
            self.join_node._check_and_execute_joins(client_id)
            return (False, True)
        
        return (False, False)