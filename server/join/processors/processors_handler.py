
from collections import defaultdict
import logging
from typing import Dict
from processors.data_processor import DataProcessor
from dtos.dto import BatchType, MenuItemBatchDTO, StoreBatchDTO, TransactionBatchDTO, TransactionItemBatchDTO, UserBatchDTO
from client_processing_state import ClientProcessingState

logger = logging.getLogger(__name__)

class ProcessorsHandler:
    def __init__(self, join_node):
        self.join_node = join_node
        self.client_states: Dict[str, ClientProcessingState] = defaultdict(ClientProcessingState)
    
    
    def handle_stores_message(self, message: bytes, client_id, message_id) -> bool:
        self.join_node._get_or_create_processors(client_id)
        dto = StoreBatchDTO.from_bytes_fast(message)
        
        if dto.batch_type == BatchType.RAW_CSV:
            csv_lines_with_prefix = []
            for line in dto.data.split('\n'):
                if line.strip():
                    csv_lines_with_prefix.append(f"stores:{line}")
            
            self.join_node.checkpoint_handler.save_message_checkpoint(
                client_id, 
                message_id, 
                csv_lines_with_prefix
            )
            
            self.join_node.store_processors[client_id].process_batch(dto.data)
            return (False, True)
        
        if dto.batch_type == BatchType.EOF:
            self.join_node.client_states[client_id].stores_loaded = True
            stores_count = len(self.join_node.store_processors[client_id].get_data())
            logger.info(f"EOF stores para '{client_id}': {stores_count} stores")
            self.join_node._check_and_execute_joins(client_id)
            return (False, True)
        
        return (False, False)
    

    def handle_users_message(self, message: bytes, client_id, message_id) -> bool:
        self.join_node._get_or_create_processors(client_id)
        
        dto = UserBatchDTO.from_bytes_fast(message)
        
        if dto.batch_type == BatchType.RAW_CSV:
            csv_lines_with_prefix = [f"users:{line}" for line in dto.data.split('\n') if line.strip()]
            
            self.join_node.checkpoint_handler.save_message_checkpoint(
                client_id, message_id, csv_lines_with_prefix
            )
            
            self.join_node.user_processors[client_id].process_batch(dto.data)
            return (False, True)
        
        if dto.batch_type == BatchType.EOF:
            self.join_node.client_states[client_id].users_loaded = True
            users_count = len(self.join_node.user_processors[client_id].get_data())
            logger.info(f"EOF users para '{client_id}': {users_count} users")
            self.join_node._check_and_execute_joins(client_id)
            return (False, True)
        
        return (False, False)

    def handle_menu_items_message(self, message: bytes, client_id, message_id) -> bool:
        self.join_node._get_or_create_processors(client_id)
        
        dto = MenuItemBatchDTO.from_bytes_fast(message)
        
        if dto.batch_type == BatchType.RAW_CSV:
            # ✅ Prefijo "menu_items:"
            csv_lines_with_prefix = [f"menu_items:{line}" for line in dto.data.split('\n') if line.strip()]
            
            self.join_node.checkpoint_handler.save_message_checkpoint(
                client_id, message_id, csv_lines_with_prefix
            )
            
            self.join_node.menu_item_processors[client_id].process_batch(dto.data)
            return (False, True)
        
        if dto.batch_type == BatchType.EOF:
            self.join_node.client_states[client_id].menu_items_loaded = True
            menu_items_count = len(self.join_node.menu_item_processors[client_id].get_data())
            logger.info(f"EOF menu_items para '{client_id}': {menu_items_count} items")
            self.join_node._check_and_execute_joins(client_id)
            return (False, True)
        
        return (False, False)
    

    

    
