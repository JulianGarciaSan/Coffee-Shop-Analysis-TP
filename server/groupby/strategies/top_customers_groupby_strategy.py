import logging
from collections import defaultdict
from typing import Dict
from .base_strategy import GroupByStrategy
from .user_purchase_count import UserPurchaseCount

logger = logging.getLogger(__name__)


class TopCustomersGroupByStrategy(GroupByStrategy):
    def __init__(self, input_queue_name: str):
        super().__init__()
        self.input_queue_name = input_queue_name
        self.store_user_purchases: Dict[str, Dict[str, UserPurchaseCount]] = defaultdict(
            lambda: defaultdict(UserPurchaseCount)
        )
        logger.info(f"TopCustomersGroupByStrategy inicializada para queue {input_queue_name}")
    
    def process_csv_line(self, csv_line: str, client_id: str = 'default_client'):
        try:
            store_id = self.dto_helper.get_column_value(csv_line, 'store_id')
            user_id = self.dto_helper.get_column_value(csv_line, 'user_id')
            
            if not store_id or not user_id or user_id.strip() == '':
                return
            
            if user_id not in self.store_user_purchases[store_id]:
                self.store_user_purchases[store_id][user_id] = UserPurchaseCount(user_id)
            
            self.store_user_purchases[store_id][user_id].add_purchase()
            
        except (ValueError, IndexError) as e:
            logger.warning(f"Error procesando línea: {e}")