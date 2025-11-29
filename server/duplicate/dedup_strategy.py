# dedup_strategy.py
from typing import Dict, Set
import logging

logger = logging.getLogger(__name__)


class DedupStrategy:
    """
    Estrategia para tracking de mensajes procesados en FilterDuplicateNode.
    Compatible con CheckpointHandler para persistencia y recovery.
    """
    
    def __init__(self):
        self.processed_by_client: Dict[str, Set[int]] = {}
        
        self.eof_clients: Set[str] = set()
        
        logger.info("DedupStrategy inicializada")
    
    
    def is_duplicate(self, client_id: str, message_id: int) -> bool:
        """
        Verifica si un mensaje ya fue procesado.
        
        Args:
            client_id: ID del cliente
            message_id: ID del mensaje (int)
            
        Returns:
            True si ya fue procesado, False si es nuevo
        """
        if client_id not in self.processed_by_client:
            return False
        
        is_dup = message_id in self.processed_by_client[client_id]
        
        if is_dup:
            logger.debug(f"Duplicado detectado: {client_id}:{message_id}")
        
        return is_dup
    
    def mark_as_processed(self, client_id: str, message_id: int):
        """
        Marca un mensaje como procesado.
        
        Args:
            client_id: ID del cliente
            message_id: ID del mensaje (int)
        """
        if client_id not in self.processed_by_client:
            self.processed_by_client[client_id] = set()
        
        self.processed_by_client[client_id].add(message_id)
        logger.debug(f"Mensaje marcado como procesado: {client_id}:{message_id}")
    
    def mark_eof(self, client_id: str):
        """
        Marca que un cliente ya envió EOF.
        
        Args:
            client_id: ID del cliente
        """
        self.eof_clients.add(client_id)
        logger.info(f"EOF marcado para cliente {client_id}")
    
    def is_eof_received(self, client_id: str) -> bool:
        """
        Verifica si un cliente ya envió EOF.
        
        Args:
            client_id: ID del cliente
            
        Returns:
            True si EOF fue recibido, False en caso contrario
        """
        return client_id in self.eof_clients
    
    def get_stats(self, client_id: str = None) -> dict:
        """
        Obtiene estadísticas de procesamiento.
        
        Args:
            client_id: Si se especifica, stats de ese cliente. Si no, de todos.
            
        Returns:
            Dict con estadísticas
        """
        if client_id:
            return {
                'client_id': client_id,
                'messages_processed': len(self.processed_by_client.get(client_id, set())),
                'eof_received': client_id in self.eof_clients
            }
        else:
            total_messages = sum(len(ids) for ids in self.processed_by_client.values())
            return {
                'total_clients': len(self.processed_by_client),
                'total_messages_processed': total_messages,
                'clients_with_eof': len(self.eof_clients)
            }
    
    
    def serialize_operation(self, client_id: str, csv_line: str) -> str:
        """
        Serializa una operación para el WAL log.
        
        Para dedup, la "csv_line" es simplemente el message_id como string.
        
        Formato: client_id,message_id
        Ejemplo: "0,8273"
        
        Args:
            client_id: ID del cliente
            csv_line: En este caso, solo el message_id como string
            
        Returns:
            String serializado para el log, o None si hay error
        """
        try:
            message_id = csv_line.strip()
            
            if not message_id:
                logger.warning("csv_line vacío en serialize_operation")
                return None
            
            try:
                int(message_id)
            except ValueError:
                logger.warning(f"message_id no numérico: {message_id}")
                return None
            
            return f"{client_id},{message_id}"
            
        except Exception as e:
            logger.error(f"Error serializando operación dedup: {e}")
            return None
    
    def deserialize_operation(self, operation_str: str) -> dict:
        """
        Deserializa una operación desde el WAL log.
        
        Input: "0,8273"
        Output: {'client_id': '0', 'message_id': 8273}
        
        Args:
            operation_str: String con formato "client_id,message_id"
            
        Returns:
            Dict con client_id y message_id
            
        Raises:
            ValueError: Si el formato es inválido
        """
        try:
            parts = operation_str.split(',')
            
            if len(parts) != 2:
                raise ValueError(f"Formato inválido, esperaba 2 campos, encontró {len(parts)}: {operation_str}")
            
            client_id, message_id_str = parts
            
            return {
                'client_id': client_id,
                'message_id': int(message_id_str)
            }
            
        except ValueError as e:
            logger.error(f"Error deserializando operación dedup: {e}")
            raise
        except Exception as e:
            logger.error(f"Error inesperado deserializando: {e}")
            raise
    
    def apply_operation(self, operation: dict):
        """
        Aplica una operación al estado en memoria.
        
        Esto se usa durante recovery para reconstruir el estado desde el log.
        
        Args:
            operation: Dict con 'client_id' y 'message_id'
            
        Raises:
            Exception: Si hay error aplicando la operación
        """
        try:
            client_id = operation['client_id']
            message_id = operation['message_id']
            
            self.mark_as_processed(client_id, message_id)
            
            logger.debug(f"Operación aplicada durante recovery: {client_id}:{message_id}")
            
        except KeyError as e:
            logger.error(f"Operación con campos faltantes: {e}")
            raise
        except Exception as e:
            logger.error(f"Error aplicando operación dedup: {e}")
            raise
    
    def _serialize_client_data(self, client_id: str) -> dict:
        """
        Serializa el estado completo de un cliente para snapshot/checkpoint.
        
        Args:
            client_id: ID del cliente
            
        Returns:
            Dict con el estado completo del cliente
        """
        processed_ids = list(self.processed_by_client.get(client_id, set()))
        eof_received = client_id in self.eof_clients
        
        serialized = {
            'processed_ids': processed_ids,
            'eof_received': eof_received,
            'total_messages': len(processed_ids)
        }
        
        logger.debug(f"Serializado estado de cliente {client_id}: {len(processed_ids)} IDs")
        
        return serialized
    
    def _deserialize_client_data(self, client_id: str, data: dict):
        """
        Reconstruye el estado de un cliente desde un snapshot/checkpoint.
        
        Args:
            client_id: ID del cliente
            data: Dict con el estado serializado
        """
        try:
            processed_ids = data.get('processed_ids', [])
            
            self.processed_by_client[client_id] = set(processed_ids)
            
            if data.get('eof_received', False):
                self.eof_clients.add(client_id)
            
            logger.info(f"Estado deserializado para cliente {client_id}: "
                       f"{len(processed_ids)} IDs procesados, "
                       f"EOF={'✓' if client_id in self.eof_clients else '✗'}")
            
        except Exception as e:
            logger.error(f"Error deserializando datos del cliente {client_id}: {e}")
            raise
    
    def _message_id_to_int(self, message_id: str) -> int:
        """
        Helper para CheckpointHandler: extrae el ID numérico de un message_id.
        
        Soporta formatos:
        - "8273" → 8273
        - "0_8273" → 8273
        - "8273:EOF" → 8273
        
        Args:
            message_id: String con el message_id
            
        Returns:
            ID numérico del mensaje
            
        Raises:
            ValueError: Si no se puede extraer un número válido
        """
        try:
            base_id = message_id.split(':')[0]
            
            if '_' in base_id:
                base_id = base_id.split('_')[-1]
            
            return int(base_id)
            
        except (ValueError, IndexError) as e:
            logger.error(f"No se pudo extraer ID numérico de '{message_id}': {e}")
            raise ValueError(f"Formato de message_id inválido: {message_id}")
    
    def clear_client(self, client_id: str):
        """
        Limpia el estado de un cliente (útil para testing o cleanup).
        
        Args:
            client_id: ID del cliente a limpiar
        """
        if client_id in self.processed_by_client:
            del self.processed_by_client[client_id]
        
        if client_id in self.eof_clients:
            self.eof_clients.remove(client_id)
        
        logger.info(f"Estado de cliente {client_id} limpiado")
    
    def clear_all(self):
        """
        Limpia todo el estado (útil para testing).
        """
        self.processed_by_client.clear()
        self.eof_clients.clear()
        logger.info("Todo el estado de DedupStrategy limpiado")