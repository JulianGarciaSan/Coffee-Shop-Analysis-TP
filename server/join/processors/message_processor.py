from collections import defaultdict
import logging
from typing import Dict, Callable, Tuple, List
from abc import ABC, abstractmethod
from processors.data_processor import DataProcessor
from dtos.dto import BatchType, MenuItemBatchDTO, StoreBatchDTO, TransactionBatchDTO, TransactionItemBatchDTO, UserBatchDTO
from client_processing_state import ClientProcessingState

logger = logging.getLogger(__name__)

class MessageProcessor:
    """Procesador genérico de mensajes con lógica común."""
    
    def __init__(self, join_node, data_type: str, dto_class, header_line: str, state_setter: Callable):
        self.join_node = join_node
        self.data_type = data_type
        self.dto_class = dto_class
        self.header_line = header_line
        self.state_setter = state_setter
    
    def handle_message(self, message: bytes, client_id: str, message_id: str) -> Tuple[bool, bool]:
        """Template method que define el flujo común."""
        self.join_node._get_or_create_processors(client_id)
        dto = self.dto_class.from_bytes_fast(message)
        
        if dto.batch_type == BatchType.RAW_CSV:
            return self._handle_csv_data(dto, client_id, message_id)
        
        if dto.batch_type == BatchType.EOF:
            return self._handle_eof(client_id, message_id)
        
        return (False, False)
    
    def _handle_csv_data(self, dto, client_id: str, message_id: str) -> Tuple[bool, bool]:
        """Procesa datos CSV eliminando headers."""
        lines = dto.data.split('\n')
        csv_lines_with_prefix = []
        
        for line in lines:
            if line.strip():
                if line.strip() == self.header_line:
                    continue
                csv_lines_with_prefix.append(f"{self.data_type}:{line.strip()}")
        
        processor = self._get_processor(client_id)
        processor.process_batch(dto.data)
        
        self.join_node.checkpoint_handler.save_message_checkpoint(
            client_id, message_id, csv_lines_with_prefix
        )
        
        return (False, True)
    
    def _handle_eof(self, client_id: str, message_id: str) -> Tuple[bool, bool]:
        """Procesa EOF y actualiza estado."""
        self.state_setter(self.join_node.client_states[client_id])
        
        processor = self._get_processor(client_id)
        count = len(processor.get_data())
        logger.info(f"EOF {self.data_type} para '{client_id}': {count} registros")
        
        self.join_node._check_and_execute_joins(client_id)
        
        lines = [f"EOF:{self.data_type}"]
        self.join_node.checkpoint_handler.save_message_checkpoint(
            client_id, message_id, lines
        )
        
        return (False, True)
    
    def _get_processor(self, client_id: str):
        """Obtiene el processor específico para este tipo de dato."""
        processors_map = {
            'stores': self.join_node.store_processors,
            'users': self.join_node.user_processors,
            'menu_items': self.join_node.menu_item_processors
        }
        return processors_map[self.data_type][client_id]