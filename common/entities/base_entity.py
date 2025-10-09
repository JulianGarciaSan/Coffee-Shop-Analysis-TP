from abc import ABC, abstractmethod
from typing import Optional
import logging

logger = logging.getLogger(__name__)

class BaseEntity(ABC):
    """Abstract base class for all entity types in the Coffee Shop Analysis system"""
    
    @classmethod
    @abstractmethod
    def parse_line(cls, line: str) -> Optional['BaseEntity']:
        """
        Parse a single CSV line into an entity instance
        
        Args:
            line: CSV line string to parse
            
        Returns:
            Entity instance if parsing successful, None otherwise
        """
        pass
    
    @classmethod
    @abstractmethod
    def get_file_type(cls) -> str:
        """
        Get the file type identifier for this entity
        
        Returns:
            Single character file type (A, B, C, D, E, F, G)
        """
        pass
    
    @classmethod
    @abstractmethod
    def get_expected_field_count(cls) -> int:
        """
        Get the expected number of fields in a CSV line for this entity
        
        Returns:
            Number of expected fields
        """
        pass
    
    @classmethod
    def get_entity_name(cls) -> str:
        """
        Get the human-readable name of this entity type
        
        Returns:
            Entity name string
        """
        return cls.__name__
    
    @classmethod
    def _safe_parse_int(cls, value: str) -> Optional[int]:
        """Safely parse string to int, return None if invalid"""
        try:
            return int(value.strip())
        except (ValueError, AttributeError):
            logger.error(f"Invalid integer value: '{value}'")
            return None
    
    @classmethod
    def _safe_parse_float(cls, value: str) -> Optional[float]:
        """Safely parse string to float, return None if invalid"""
        try:
            return float(value.strip())
        except (ValueError, AttributeError):
            logger.error(f"Invalid float value: '{value}'")
            return None
    
    @classmethod
    def _safe_parse_bool(cls, value: str) -> bool:
        """Safely parse string to boolean"""
        return value.strip().lower() in ('true', '1', 'yes', 'y')
    
    @classmethod
    def _validate_field_count(cls, parts: list, line: str) -> bool:
        """Validate that the line has the expected number of fields"""
        expected = cls.get_expected_field_count()
        if len(parts) < expected:
            logger.warning(f"Invalid {cls.get_entity_name()} data - expected {expected} fields, got {len(parts)}: {line}")
            return False
        return True