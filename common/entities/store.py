from dataclasses import dataclass
from typing import Optional
from .base_entity import BaseEntity
import logging

logger = logging.getLogger(__name__)

@dataclass
class Store(BaseEntity):
    """Store entity - represents coffee shop store locations"""
    
    store_id: int
    store_name: str
    street: str
    postal_code: str
    city: str
    state: str
    latitude: float
    
    @classmethod
    def parse_line(cls, line: str) -> Optional['Store']:
        """Parse a CSV line into a Store instance"""
        try:
            parts = line.split(',')
            if not cls._validate_field_count(parts, line):
                return None
            
            store_id = cls._safe_parse_int(parts[0])
            if store_id is None:
                return None
                
            latitude = cls._safe_parse_float(parts[6])
            if latitude is None:
                return None
            
            return Store(
                store_id=store_id,
                store_name=parts[1].strip(),
                street=parts[2].strip(),
                postal_code=parts[3].strip(),
                city=parts[4].strip(),
                state=parts[5].strip(),
                latitude=latitude
            )
        except (IndexError, ValueError) as e:
            logger.error(f"Error parsing Store line '{line}': {e}")
            return None
    
    @classmethod
    def get_file_type(cls) -> str:
        """Store corresponds to file type C"""
        return 'C'
    
    @classmethod
    def get_expected_field_count(cls) -> int:
        """Store expects 7 fields"""
        return 7