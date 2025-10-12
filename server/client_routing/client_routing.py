import logging
from typing import List

logger = logging.getLogger(__name__)


class ClientRouter:
    def __init__(self, total_join_nodes: int, node_prefix: str = "join_node"):
        self.total_join_nodes = total_join_nodes
        self.node_prefix = node_prefix
        
        self.join_nodes = [f"{node_prefix}_{i}" for i in range(total_join_nodes)]
        
        logger.info(f"ClientRouter inicializado: {total_join_nodes} nodos")
        logger.info(f"Nodos: {self.join_nodes}")
    
    def get_node_for_client(self, client_id: str) -> str:
        try:
            node_index = int(client_id) % self.total_join_nodes
        except (ValueError, TypeError):
            logger.warning(f"Client_id '{client_id}' no es numérico, usando hash")
            node_index = hash(str(client_id)) % self.total_join_nodes
        
        assigned_node = self.join_nodes[node_index]
        
        logger.debug(f"Cliente '{client_id}' → Nodo '{assigned_node}' (índice {node_index})")
        
        return assigned_node
    
    def get_routing_key(self, client_id: str, base_routing_key: str) -> str:
        node = self.get_node_for_client(client_id)
        return f"{node}.{base_routing_key}"
    
    def get_all_routing_keys(self, base_routing_key: str) -> List[str]:
        return [f"{node}.{base_routing_key}" for node in self.join_nodes]
