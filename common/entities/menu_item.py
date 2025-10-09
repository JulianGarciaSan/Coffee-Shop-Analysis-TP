from dataclasses import dataclass
from typing import Optional
from .base_entity import BaseEntity

@dataclass
class MenuItem(BaseEntity):
    """Menu Item entity - represents items available in the coffee shop menu"""
    
    item_id: int
    item_name: str
    category: str
    price: float
    is_seasonal: bool
    available_from: str
    available_to: str
    
    @classmethod
    def parse_line(cls, line: str) -> Optional['MenuItem']:
        """Parse a CSV line into a MenuItem instance"""
        try:
            parts = line.split(',')
            if not cls._validate_field_count(parts, line):
                return None
            
            item_id = cls._safe_parse_int(parts[0])
            if item_id is None:
                return None
                
            price = cls._safe_parse_float(parts[3])
            if price is None:
                return None
            
            return MenuItem(
                item_id=item_id,
                item_name=parts[1].strip(),
                category=parts[2].strip(),
                price=price,
                is_seasonal=cls._safe_parse_bool(parts[4]),
                available_from=parts[5].strip(),
                available_to=parts[6].strip()
            )
        except (IndexError, ValueError) as e:
            cls._log_parse_error(line, e)
            return None
    
    @classmethod
    def get_file_type(cls) -> str:
        """MenuItem corresponds to file type A"""
        return 'A'
    
    @classmethod
    def get_expected_field_count(cls) -> int:
        """MenuItem expects 7 fields"""
        return 7
    
    @classmethod
    def _log_parse_error(cls, line: str, error: Exception):
        """Log parsing error with context"""
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Error parsing MenuItem line '{line}': {error}")