from dataclasses import dataclass
from typing import Optional
from .base_entity import BaseEntity
import logging

logger = logging.getLogger(__name__)

@dataclass
class Voucher(BaseEntity):
    """Voucher entity - represents discount vouchers available to customers"""
    
    voucher_id: int
    voucher_code: str
    discount_type: str
    discount_value: float
    valid_from: str
    valid_to: str
    
    @classmethod
    def parse_line(cls, line: str) -> Optional['Voucher']:
        """Parse a CSV line into a Voucher instance"""
        try:
            parts = line.split(',')
            if not cls._validate_field_count(parts, line):
                return None
            
            voucher_id = cls._safe_parse_int(parts[0])
            if voucher_id is None:
                return None
                
            discount_value = cls._safe_parse_float(parts[3])
            if discount_value is None:
                return None
            
            return Voucher(
                voucher_id=voucher_id,
                voucher_code=parts[1].strip(),
                discount_type=parts[2].strip(),
                discount_value=discount_value,
                valid_from=parts[4].strip(),
                valid_to=parts[5].strip()
            )
        except (IndexError, ValueError) as e:
            logger.error(f"Error parsing Voucher line '{line}': {e}")
            return None
    
    @classmethod
    def get_file_type(cls) -> str:
        """Voucher corresponds to file type E"""
        return 'E'
    
    @classmethod
    def get_expected_field_count(cls) -> int:
        """Voucher expects 6 fields"""
        return 6