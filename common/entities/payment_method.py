from dataclasses import dataclass
from typing import Optional
from .base_entity import BaseEntity
import logging

logger = logging.getLogger(__name__)

@dataclass
class PaymentMethod(BaseEntity):
    """Payment Method entity - represents available payment methods"""
    
    payment_method_id: int
    payment_method_name: str
    processing_fee: float
    
    @classmethod
    def parse_line(cls, line: str) -> Optional['PaymentMethod']:
        """Parse a CSV line into a PaymentMethod instance"""
        try:
            parts = line.split(',')
            if not cls._validate_field_count(parts, line):
                return None
            
            payment_method_id = cls._safe_parse_int(parts[0])
            if payment_method_id is None:
                return None
                
            processing_fee = cls._safe_parse_float(parts[2])
            if processing_fee is None:
                return None
            
            return PaymentMethod(
                payment_method_id=payment_method_id,
                payment_method_name=parts[1].strip(),
                processing_fee=processing_fee
            )
        except (IndexError, ValueError) as e:
            logger.error(f"Error parsing PaymentMethod line '{line}': {e}")
            return None
    
    @classmethod
    def get_file_type(cls) -> str:
        """PaymentMethod corresponds to file type B"""
        return 'B'
    
    @classmethod
    def get_expected_field_count(cls) -> int:
        """PaymentMethod expects 3 fields"""
        return 3