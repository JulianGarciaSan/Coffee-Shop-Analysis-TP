from dataclasses import dataclass
from typing import Optional
from .base_entity import BaseEntity
import logging

logger = logging.getLogger(__name__)

@dataclass
class Transaction(BaseEntity):
    """Transaction entity - represents complete transaction records"""
    
    transaction_id: str
    store_id: int
    payment_method_id: int
    voucher_id: int
    user_id: int
    original_amount: float
    discount_applied: float
    final_amount: float
    created_at: str
    
    @classmethod
    def parse_line(cls, line: str) -> Optional['Transaction']:
        """Parse a CSV line into a Transaction instance"""
        try:
            parts = line.split(',')
            if not cls._validate_field_count(parts, line):
                return None
            
            store_id = cls._safe_parse_int(parts[1])
            if store_id is None:
                return None
                
            payment_method_id = cls._safe_parse_int(parts[2])
            if payment_method_id is None:
                return None
                
            voucher_id = cls._safe_parse_int(parts[3])
            if voucher_id is None:
                return None
                
            user_id = cls._safe_parse_int(parts[4])
            if user_id is None:
                return None
                
            original_amount = cls._safe_parse_float(parts[5])
            if original_amount is None:
                return None
                
            discount_applied = cls._safe_parse_float(parts[6])
            if discount_applied is None:
                return None
                
            final_amount = cls._safe_parse_float(parts[7])
            if final_amount is None:
                return None
            
            return Transaction(
                transaction_id=parts[0].strip(),
                store_id=store_id,
                payment_method_id=payment_method_id,
                voucher_id=voucher_id,
                user_id=user_id,
                original_amount=original_amount,
                discount_applied=discount_applied,
                final_amount=final_amount,
                created_at=parts[8].strip()
            )
        except (IndexError, ValueError) as e:
            logger.error(f"Error parsing Transaction line '{line}': {e}")
            return None
    
    @classmethod
    def get_file_type(cls) -> str:
        """Transaction corresponds to file type G"""
        return 'G'
    
    @classmethod
    def get_expected_field_count(cls) -> int:
        """Transaction expects 9 fields"""
        return 9