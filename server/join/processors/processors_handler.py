from collections import defaultdict
import logging
from typing import Dict, Callable, Tuple, List
from abc import ABC, abstractmethod
from processors.data_processor import DataProcessor
from dtos.dto import BatchType, MenuItemBatchDTO, StoreBatchDTO, TransactionBatchDTO, TransactionItemBatchDTO, UserBatchDTO
from client_processing_state import ClientProcessingState
from processors.message_processor import MessageProcessor
logger = logging.getLogger(__name__)

class ProcessorsHandler:
    def __init__(self, join_node):
        self.join_node = join_node
        self.client_states: Dict[str, ClientProcessingState] = defaultdict(ClientProcessingState)
        
        self.processors = {
            'stores': MessageProcessor(
                join_node=join_node,
                data_type='stores',
                dto_class=StoreBatchDTO,
                header_line='store_id,store_name',
                state_setter=lambda state: setattr(state, 'stores_loaded', True)
            ),
            'users': MessageProcessor(
                join_node=join_node,
                data_type='users',
                dto_class=UserBatchDTO,
                header_line='user_id,birthdate',
                state_setter=lambda state: setattr(state, 'users_loaded', True)
            ),
            'menu_items': MessageProcessor(
                join_node=join_node,
                data_type='menu_items',
                dto_class=MenuItemBatchDTO,
                header_line='item_id,item_name',
                state_setter=lambda state: setattr(state, 'menu_items_loaded', True)
            )
        }
    
    def handle_stores_message(self, message: bytes, client_id: str, message_id: str) -> Tuple[bool, bool]:
        return self.processors['stores'].handle_message(message, client_id, message_id)
    
    def handle_users_message(self, message: bytes, client_id: str, message_id: str) -> Tuple[bool, bool]:
        return self.processors['users'].handle_message(message, client_id, message_id)
    
    def handle_menu_items_message(self, message: bytes, client_id: str, message_id: str) -> Tuple[bool, bool]:
        return self.processors['menu_items'].handle_message(message, client_id, message_id)