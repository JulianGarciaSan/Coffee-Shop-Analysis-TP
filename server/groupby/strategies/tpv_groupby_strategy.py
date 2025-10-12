import logging
from collections import defaultdict
from typing import Dict, Tuple
from server.groupby.strategies.base_strategy import GroupByStrategy
from server.groupby.configurators.tpv_aggregation import TPVAggregation
from dtos.dto import TransactionBatchDTO, BatchType
from rabbitmq.middleware import MessageMiddlewareExchange

logger = logging.getLogger(__name__)


class TPVGroupByStrategy(GroupByStrategy):
    def __init__(self, semester: str):
        super().__init__()
        self.semester = semester
        self.tpv_aggregations: Dict[Tuple[str, str], TPVAggregation] = defaultdict(TPVAggregation)
        logger.info(f"TPVGroupByStrategy inicializada para semestre {semester}")
    
    
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
