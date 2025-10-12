import logging
from collections import defaultdict
from typing import Dict, Tuple
from .base_strategy import GroupByStrategy
from configurators.tpv_aggregation import TPVAggregation

logger = logging.getLogger(__name__)


class TPVGroupByStrategy(GroupByStrategy):
    def __init__(self, semester: str):
        super().__init__()
        self.semester = semester
        self.tpv_aggregations_by_client: Dict[str, Dict[Tuple[str, str], TPVAggregation]] = defaultdict(
            lambda: defaultdict(TPVAggregation)
        )
        self.eof_received_by_client: Dict[str, bool] = {}

        logger.info(f"TPVGroupByStrategy inicializada para semestre {self.semester}")
    
    
    def process_csv_line(self, csv_line: str, client_id: str = 'default_client'):
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
            self.tpv_aggregations_by_client[client_id][key].add_transaction(final_amount)
            
        except (ValueError, IndexError) as e:
            logger.warning(f"Error procesando línea para TPV (cliente '{client_id}'): {e}")
    
    def generate_results_csv_for_client(self, client_id: str) -> str:
        client_aggregations = self.tpv_aggregations_by_client.get(client_id, {})
        
        if not client_aggregations:
            logger.warning(f"No hay datos TPV para cliente '{client_id}'")
            return "year_half_created_at,store_id,total_payment_value,transaction_count"
        
        csv_lines = ["year_half_created_at,store_id,total_payment_value,transaction_count"]
        
        for (year_half, store_id) in sorted(client_aggregations.keys()):
            aggregation = client_aggregations[(year_half, store_id)]
            csv_lines.append(aggregation.to_csv_line(year_half, store_id))
        
        logger.info(f"Resultados TPV generados para cliente '{client_id}': {len(client_aggregations)} grupos")
        return '\n'.join(csv_lines)
