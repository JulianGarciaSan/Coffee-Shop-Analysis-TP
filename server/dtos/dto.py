from abc import ABC, abstractmethod
import struct
import json
from typing import Dict, List, Any, Optional
from enum import Enum


class FileType(Enum):
    TRANSACTIONS = "transactions"
    USERS = "users"
    STORES = "stores"
    MENU_ITEMS = "menu_items"
    PAYMENT_METHODS = "payment_methods"
    VOUCHERS = "vouchers"
    TRANSACTION_ITEMS = "transaction_items"
    REPORT_BATCH = "report_batch"


class BatchType(Enum):
    DATA = "DATA"
    CONTROL = "CONTROL"
    RAW_CSV = "RAW_CSV"
    EOF = "EOF"


class BaseDTO(ABC):
    
    def __init__(self, data, batch_type: BatchType = BatchType.DATA, file_type: FileType = None):
        self.data = data
        self.batch_type = batch_type
        self.file_type = file_type
        
    @abstractmethod
    def get_column_index(self, column_name: str) -> int:
        pass
    
        
    def get_column_value(self, csv_line: str, column_name: str) -> str:
        try:
            parts = csv_line.split(',')
            column_index = self.get_column_index(column_name)
            
            if column_index >= len(parts):
                return ""
            
            return parts[column_index].strip()
            
        except (IndexError, ValueError):
            return ""
    
    @abstractmethod
    def get_csv_headers(self) -> List[str]:
        pass
    
    @abstractmethod
    def dict_to_csv_line(self, record: Dict) -> str:
        pass
    
    @abstractmethod
    def csv_line_to_dict(self, csv_line: str) -> Dict:
        pass
    
    def to_bytes_fast(self) -> bytes:
        if self.batch_type in [BatchType.EOF, BatchType.RAW_CSV]:
            return self.data.encode('utf-8')
        else:
            return
        
    
    
    @classmethod
    def from_bytes_fast(cls, data: bytes) -> 'BaseDTO':
        decoded_data = data.decode('utf-8').strip()
        
        if decoded_data.startswith("EOF:"):
            return cls(decoded_data, BatchType.EOF)
        
        return cls(decoded_data, BatchType.RAW_CSV)
    
    def to_bytes_fast(self) -> bytes:
        return self.data.encode('utf-8')


class TransactionBatchDTO(BaseDTO): 
    def __init__(self, transactions, batch_type="DATA"):
        if isinstance(batch_type, str):
            batch_type = BatchType(batch_type)
        super().__init__(transactions, batch_type, FileType.TRANSACTIONS)


    def get_csv_headers(self) -> List[str]:
        return [
            "transaction_id", "store_id","user_id", "final_amount", "created_at"
        ]
    
    def dict_to_csv_line(self, record: Dict) -> str:
        return (f"{record['transaction_id']},{record['store_id']},{record['user_id']},"
                f"{record['final_amount']},{record['created_at']}")
    
    def csv_line_to_dict(self, csv_line: str) -> Dict:
        values = csv_line.split(',')
        return {
            "transaction_id": values[0],
            "store_id": values[1],
            "user_id": values[2],
            "final_amount": values[3],
            "created_at": values[4],
        }
    def get_column_index(self, column_name: str) -> int:
        column_map = {
            'transaction_id': 0,
            'store_id': 1,
            'user_id': 2,
            'final_amount': 3,
            'created_at': 4
        }
        
        if column_name not in column_map:
            raise ValueError(f"Columna '{column_name}' no existe en transacciones")
        
        return column_map[column_name]
    
    def filter_columns(self):
        """Filtra y deja solo las columnas necesarias"""
        lines = self.data.split('\n')
        filtered_lines = []
        
        for line in lines:
            if line.strip():
                parts = line.split(',')
                if len(parts) >= 9:
                    filtered_line = f"{parts[0]},{parts[1]},{parts[4]},{parts[7]},{parts[8]}"
                    filtered_lines.append(filtered_line)
        
        self.data = '\n'.join(filtered_lines)
        return self
    


class UserBatchDTO(BaseDTO):
    def __init__(self, users, batch_type=BatchType.DATA):
        super().__init__(users, batch_type, FileType.USERS)
    
    def get_csv_headers(self) -> List[str]:
        return ["user_id", "gender", "birthdate", "registered_at"]
    
    def dict_to_csv_line(self, record: Dict) -> str:
        return (f"{record['user_id']},{record['birthdate']}")
    
    def csv_line_to_dict(self, csv_line: str) -> Dict:
        values = csv_line.split(',')
        return {
            "user_id": values[0],
            "birthdate": values[2],
        }
    
    def get_column_index(self, column_name: str) -> int:
        column_map = {
            'user_id': 0,
            'birthdate': 1,
        }
        
        if column_name not in column_map:
            raise ValueError(f"Columna '{column_name}' no existe en users")
        
        return column_map[column_name]
    
    def filter_columns(self):
        """Filtra y deja solo las columnas necesarias"""
        lines = self.data.split('\n')
        filtered_lines = []
        
        for line in lines:
            if line.strip():
                parts = line.split(',')
                if len(parts) >= 3:
                    filtered_line = f"{parts[0]},{parts[2]}"
                    filtered_lines.append(filtered_line)
        
        self.data = '\n'.join(filtered_lines)
        return self


class StoreBatchDTO(BaseDTO):
    
    def __init__(self, stores, batch_type=BatchType.DATA):
        super().__init__(stores, batch_type, FileType.STORES)
    
    def get_csv_headers(self) -> List[str]:
        return ["store_id", "store_name", "street", "postal_code", "city","state","latitude","longitude"]
    
    def dict_to_csv_line(self, record: Dict) -> str:
        return (f"{record['store_id']},{record['store_name']}")
    
    def csv_line_to_dict(self, csv_line: str) -> Dict:
        values = csv_line.split(',')
        return {
            "store_id": values[0],
            "store_name": values[1],
        }
        
    def get_column_index(self, column_name: str) -> int:
        column_map = {
            'store_id': 0,
            'store_name': 1,
        }
        
        if column_name not in column_map:
            raise ValueError(f"Columna '{column_name}' no existe en stores")
        
        return column_map[column_name]
    
    def filter_columns(self):
        """Filtra y deja solo las columnas necesarias"""
        lines = self.data.split('\n')
        filtered_lines = []
        
        for line in lines:
            if line.strip():
                parts = line.split(',')
                if len(parts) >= 2:
                    filtered_line = f"{parts[0]},{parts[1]}"
                    filtered_lines.append(filtered_line)
        
        self.data = '\n'.join(filtered_lines)
        return self


class MenuItemBatchDTO(BaseDTO):
    def __init__(self, menu_items, batch_type=BatchType.DATA):
        super().__init__(menu_items, batch_type, FileType.MENU_ITEMS)
    
    def get_csv_headers(self) -> List[str]:
        return ["item_id", "item_name"]
    
    def dict_to_csv_line(self, record: Dict) -> str:
        return (f"{record['item_id']},{record['item_name']}")
    def csv_line_to_dict(self, csv_line: str) -> Dict:
        values = csv_line.split(',')
        return {
            "item_id": values[0],
            "item_name": values[1],
        }
        
    def get_column_index(self, column_name: str) -> int:
        column_map = {
            'item_id': 0,
            'item_name': 1,
        }
        
        if column_name not in column_map:
            raise ValueError(f"Columna '{column_name}' no existe en transaction_items")
        
        return column_map[column_name]

    def filter_columns(self):
        """Filtra y deja solo las columnas necesarias"""
        lines = self.data.split('\n')
        filtered_lines = []
        
        for line in lines:
            if line.strip():
                parts = line.split(',')
                if len(parts) >= 2:
                    filtered_line = f"{parts[0]},{parts[1]}"
                    filtered_lines.append(filtered_line)
        
        self.data = '\n'.join(filtered_lines)
        return self

class TransactionItemBatchDTO(BaseDTO):
    def __init__(self, transaction_items, batch_type=BatchType.DATA):
        super().__init__(transaction_items, batch_type, FileType.TRANSACTION_ITEMS)
    
    def get_csv_headers(self) -> List[str]:
        return ["item_id", "quantity", "subtotal", "created_at"]

    def dict_to_csv_line(self, record: Dict) -> str:
        return (f"{record['item_id']},{record['quantity']},"
                f"{record['subtotal']},{record['created_at']}")
    
    def csv_line_to_dict(self, csv_line: str) -> Dict:
        values = csv_line.split(',')
        return {
            "item_id": values[0],
            "quantity": values[1],
            "subtotal": values[2],
            "created_at": values[3]
        }
    
    def get_column_index(self, column_name: str) -> int:
        column_map = {
            'item_id': 0,
            'quantity': 1,
            'subtotal': 2,
            'created_at': 3
        }
        
        if column_name not in column_map:
            raise ValueError(f"Columna '{column_name}' no existe en transaction_items")
        
        return column_map[column_name]
    
    def filter_columns(self):
        """Filtra y deja solo las columnas necesarias"""
        lines = self.data.split('\n')
        filtered_lines = []
        
        for line in lines:
            if line.strip():
                parts = line.split(',')
                if len(parts) >= 6:
                    filtered_line = f"{parts[1]},{parts[2]},{parts[4]},{parts[5]}"
                    filtered_lines.append(filtered_line)
        
        self.data = '\n'.join(filtered_lines)
        return self


class ReportBatchDTO(BaseDTO):
    """
    DTO específico para batches de reportes (RAW_CSV).
    """
    
    def __init__(self, data: str, batch_type: BatchType, query_name: str = None):
        super().__init__(data, batch_type, None)
        self.query_name = query_name
    
    @classmethod
    def create_eof(cls, query_name: str):
        """Helper para crear EOF markers."""
        return cls("EOF:", BatchType.EOF, query_name)

    def get_csv_headers(self) -> List[str]:
        if self.query_name == "Q1":
            return ["store_id", "total_sales", "total_transactions"]
        elif self.query_name == "Q3":
            return ["item_id", "total_quantity_sold", "total_revenue"]
        elif self.query_name == "Q4":
            return ["user_id", "total_spent", "total_transactions"]
    
    def dict_to_csv_line(self, record: Dict) -> str:
        if self.query_name == "Q1":
            return f"{record['store_id']},{record['total_sales']},{record['total_transactions']}"
        elif self.query_name == "Q3":
            return f"{record['item_id']},{record['total_quantity_sold']},{record['total_revenue']}"
        elif self.query_name == "Q4":
            return f"{record['user_id']},{record['total_spent']},{record['total_transactions']}"
        
    def csv_line_to_dict(self, csv_line: str) -> Dict:
        if self.query_name == "Q1":
            values = csv_line.split(',')
            return {
                "store_id": values[0],
                "total_sales": values[1],
                "total_transactions": values[2]
            }
        elif self.query_name == "Q3":
            values = csv_line.split(',')
            return {
                "item_id": values[0],
                "total_quantity_sold": values[1],
                "total_revenue": values[2]
            }
        elif self.query_name == "Q4":
            values = csv_line.split(',')
            return {
                "user_id": values[0],
                "total_spent": values[1],
                "total_transactions": values[2]
            }
            
    def get_batch_name(self) -> str:
        if self.query_name == "Q1":
            return "query1.csv"
        elif self.query_name == "Q3":
            return "query3.csv"
        elif self.query_name == "Q4":
            return "query4.csv"
        else:
            return "UNKNOWN"
        
    def get_column_index(self, column_name: str) -> int:
        raise NotImplementedError("ReportBatchBatchDTO no tiene columnas específicas.")
    
class CoordinationMessageDTO(BaseDTO):
    EOF_FANOUT = "EOF_FANOUT"
    ACK = "ACK"
    
    def __init__(self, msg_type: str, client_id: str, node_id: str, 
                 batch_type: str = "transactions"):
        self.msg_type = msg_type
        self.client_id = client_id
        self.node_id = node_id
        self.batch_type_str = batch_type
        
        # Formato simple: tipo|client_id|node_id|batch_type
        data = f"{msg_type}|{client_id}|{node_id}|{batch_type}"
        
        super().__init__(data, BatchType.CONTROL, None)
    
    @classmethod
    def create_eof_fanout(cls, client_id: str, node_id: str, batch_type: str) -> 'CoordinationMessageDTO':
        """Helper para crear mensaje EOF_FANOUT"""
        return cls(cls.EOF_FANOUT, client_id, node_id, batch_type)
    
    @classmethod
    def create_ack(cls, client_id: str, node_id: str, batch_type: str) -> 'CoordinationMessageDTO':
        """Helper para crear mensaje ACK"""
        return cls(cls.ACK, client_id, node_id, batch_type)
    
    @classmethod
    def from_bytes_fast(cls, data: bytes) -> 'CoordinationMessageDTO':
        """Deserializa desde bytes (formato: tipo|client_id|node_id|batch_type)"""
        decoded_data = data.decode('utf-8').strip()
        parts = decoded_data.split('|')
        
        if len(parts) != 4:
            raise ValueError(f"Formato inválido de CoordinationMessageDTO: {decoded_data}")
        
        return cls(
            msg_type=parts[0],
            client_id=parts[1],
            node_id=parts[2],
            batch_type=parts[3]
        )
    def get_csv_headers(self) -> List[str]:
        """No aplica para mensajes de coordinación"""
        return []
    
    def dict_to_csv_line(self, record: Dict) -> str:
        """No aplica para mensajes de coordinación"""
        return ""
    
    def csv_line_to_dict(self, csv_line: str) -> Dict:
        """No aplica para mensajes de coordinación"""
        return {}
    
    def get_column_index(self, column_name: str) -> int:
        """No aplica para mensajes de coordinación"""
        raise NotImplementedError("CoordinationMessageDTO no tiene columnas")
    
class DTOFactory:
    """
    Factory para crear DTOs según el tipo de archivo.
    """
    
    _dto_mapping = {
        FileType.TRANSACTIONS: TransactionBatchDTO,
        FileType.USERS: UserBatchDTO,
        FileType.STORES: StoreBatchDTO,
        FileType.MENU_ITEMS: MenuItemBatchDTO,
        FileType.TRANSACTION_ITEMS: TransactionItemBatchDTO,
        FileType.REPORT_BATCH: ReportBatchDTO,
    }
    
    @classmethod
    def create_dto(cls, file_type: FileType, data, batch_type=BatchType.DATA) -> BaseDTO:
        """
        Crea un DTO específico según el tipo de archivo.
        
        Args:
            file_type: Tipo de archivo
            data: Datos a encapsular
            batch_type: Tipo de batch
            
        Returns:
            BaseDTO: Instancia del DTO específico
        """
        dto_class = cls._dto_mapping.get(file_type)
        if not dto_class:
            raise ValueError(f"Tipo de archivo no soportado: {file_type}")
        
        return dto_class(data, batch_type)