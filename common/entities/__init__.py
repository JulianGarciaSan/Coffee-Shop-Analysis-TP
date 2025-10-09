"""
Coffee Shop Analysis Entity Classes

This package contains entity classes for parsing different CSV file types
in the Coffee Shop Analysis distributed system.

Each entity class inherits from BaseEntity and implements:
- parse_line(cls, line: str) -> Optional[Entity]: Parse CSV line to entity
- get_file_type(cls) -> str: Get file type identifier (A-G)
- get_expected_field_count(cls) -> int: Get expected field count
"""

from .base_entity import BaseEntity
from .menu_item import MenuItem
from .payment_method import PaymentMethod
from .store import Store
from .transaction_item import TransactionItem
from .voucher import Voucher
from .user import User
from .transaction import Transaction

ENTITY_REGISTRY = {
    'A': MenuItem,
    'B': PaymentMethod,
    'C': Store,
    'D': TransactionItem,
    'E': Voucher,
    'F': User,
    'G': Transaction
}

ENTITY_CLASSES = list(ENTITY_REGISTRY.values())

def get_entity_class(file_type: str) -> type:
    if file_type not in ENTITY_REGISTRY:
        raise KeyError(f"Unsupported file type: {file_type}")
    return ENTITY_REGISTRY[file_type]

def get_entity_name(file_type: str) -> str:
    """Get entity name for a file type"""
    entity_class = get_entity_class(file_type)
    return entity_class.get_entity_name()

__all__ = [
    'BaseEntity',
    'MenuItem',
    'PaymentMethod', 
    'Store',
    'TransactionItem',
    'Voucher',
    'User',
    'Transaction',
    'ENTITY_REGISTRY',
    'ENTITY_CLASSES',
    'get_entity_class',
    'get_supported_file_types',
    'get_entity_name'
]