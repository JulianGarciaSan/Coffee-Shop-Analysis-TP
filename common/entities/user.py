from dataclasses import dataclass
from typing import Optional
from .base_entity import BaseEntity
import logging

logger = logging.getLogger(__name__)

@dataclass
class User(BaseEntity):
    """User entity - represents customer information"""
    
    user_id: int
    gender: str
    birthdate: str
    registered_at: str
    
    @classmethod
    def parse_line(cls, line: str) -> Optional['User']:
        """Parse a CSV line into a User instance"""
        try:
            parts = line.split(',')
            if not cls._validate_field_count(parts, line):
                return None
            
            user_id = cls._safe_parse_int(parts[0])
            if user_id is None:
                return None
            
            return User(
                user_id=user_id,
                gender=parts[1].strip(),
                birthdate=parts[2].strip(),
                registered_at=parts[3].strip()
            )
        except (IndexError, ValueError) as e:
            logger.error(f"Error parsing User line '{line}': {e}")
            return None
    
    @classmethod
    def get_file_type(cls) -> str:
        """User corresponds to file type F"""
        return 'F'
    
    @classmethod
    def get_expected_field_count(cls) -> int:
        """User expects 4 fields"""
        return 4