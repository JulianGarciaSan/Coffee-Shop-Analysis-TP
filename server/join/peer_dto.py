from dataclasses import dataclass
from typing import Dict
import json

@dataclass
class PeerUserRequest:
    """Solicitud de user data a otro join node"""
    requesting_node_id: int
    client_id: str
    user_ids: list
    
    def to_bytes(self) -> bytes:
        data = {
            'requesting_node_id': self.requesting_node_id,
            'client_id': self.client_id,
            'user_ids': self.user_ids
        }
        return json.dumps(data).encode('utf-8')
    
    @classmethod
    def from_bytes(cls, data: bytes):
        parsed = json.loads(data.decode('utf-8'))
        return cls(**parsed)


@dataclass
class PeerUserResponse:
    """Respuesta con user data de otro join node"""
    responding_node_id: int
    client_id: str
    users_data: Dict[str, Dict]
    
    def to_bytes(self) -> bytes:
        data = {
            'responding_node_id': self.responding_node_id,
            'client_id': self.client_id,
            'users_data': self.users_data
        }
        return json.dumps(data).encode('utf-8')
    
    @classmethod
    def from_bytes(cls, data: bytes):
        parsed = json.loads(data.decode('utf-8'))
        return cls(**parsed)