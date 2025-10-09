from dataclasses import dataclass
from typing import Optional
from .base_entity import BaseEntity
import logging

logger = logging.getLogger(__name__)

@dataclass
class TransactionItem(BaseEntity):
    """Transaction Item entity - represents individual items within a transaction"""
    
    transaction_id: str
    item_id: int
    quantity: int
    unit_price: float
    subtotal: float
    created_at: str
    
    @classmethod
    def parse_line(cls, line: str) -> Optional['TransactionItem']:
        """Parse a CSV line into a TransactionItem instance"""
        try:
            parts = line.split(',')
            if not cls._validate_field_count(parts, line):
                return None
            
            item_id = cls._safe_parse_int(parts[1])
            if item_id is None:
                return None
                
            quantity = cls._safe_parse_int(parts[2])
            if quantity is None:
                return None
                
            unit_price = cls._safe_parse_float(parts[3])
            if unit_price is None:
                return None
                
            subtotal = cls._safe_parse_float(parts[4])
            if subtotal is None:
                return None
            
            return TransactionItem(
                transaction_id=parts[0].strip(),
                item_id=item_id,
                quantity=quantity,
                unit_price=unit_price,
                subtotal=subtotal,
                created_at=parts[5].strip()
            )
        except (IndexError, ValueError) as e:
            logger.error(f"Error parsing TransactionItem line '{line}': {e}")
            return None
    
    @classmethod
    def get_file_type(cls) -> str:
        """TransactionItem corresponds to file type D"""
        return 'D'
    
    @classmethod
    def get_expected_field_count(cls) -> int:
        """TransactionItem expects 6 fields"""
        return 6