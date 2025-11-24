import json
import logging
import os
import tempfile
from typing import Any, Dict
from message_range_tracker.message_range_tracker import MessageRangeTracker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CheckpointHandler:
    def __init__(self,checkpoint_dir: str = None, node: Any = None, transactions_logger = None):
        self.message_trackers: Dict[str, MessageRangeTracker] = {}
        self.transactions_logger = transactions_logger
        self.node = node

        if checkpoint_dir:
            self.checkpoint_dir = checkpoint_dir
        else:
            self.checkpoint_dir = '/app/server/logs/checkpoints'
        
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        logger.info(f"Sistema de checkpoints inicializado en {self.checkpoint_dir}")
             
        
    def save_message_checkpoint(self, client_id: str, message_id: str):
        """
        Guarda checkpoint con rangos de message_ids procesados.
        """
        if not self.transactions_logger:
            logger.warning("No hay logger configurado, saltando checkpoint")
            return
        
        try:
            if client_id not in self.message_trackers:
                self.message_trackers[client_id] = MessageRangeTracker()
            
            try:
                msg_id_int = int(message_id)
            except ValueError:
                msg_id_int = int(message_id.split('_')[-1])
            
            self.message_trackers[client_id].add_message_id(msg_id_int)
            
            client_data = self.node._serialize_client_data(client_id)
            
            checkpoint_data = {
                "client_id": client_id,
                "last_message_id": message_id, 
                "processed_ranges": self.message_trackers[client_id].to_list(),  
                "total_messages_processed": self.message_trackers[client_id].total_messages(),
                "node_type": self.node.__class__.__name__,
                "data": client_data
            }
            
            self._atomic_write_checkpoint(client_id, message_id, checkpoint_data)
            
            logger.debug(f"Checkpoint guardado {client_id}:{message_id} "
                        f"({len(self.message_trackers[client_id])} rangos, "
                        f"{self.message_trackers[client_id].total_messages()} mensajes)")
            
        except Exception as e:
            logger.error(f"Error guardando checkpoint para {client_id}:{message_id}: {e}")
            raise
    
    def _atomic_write_checkpoint(self, client_id: str, message_id: str, checkpoint_data: dict):
        """
        Escritura atómica de checkpoint con backup automático.
        """
        checkpoint_path = os.path.join(self.checkpoint_dir, f"client_{client_id}.checkpoint")
        checkpoint_prev_path = f"{checkpoint_path}.prev"
        
        temp_fd, temp_path = tempfile.mkstemp(dir=self.checkpoint_dir, prefix='checkpoint_tmp_')
        try:
            with os.fdopen(temp_fd, 'w') as f:
                json.dump(checkpoint_data, f, indent=2)
                f.write('\n')
                f.flush()
                os.fsync(f.fileno())
            
            if os.path.exists(checkpoint_path):
                try:
                    if os.path.exists(checkpoint_prev_path):
                        os.remove(checkpoint_prev_path)
                    os.rename(checkpoint_path, checkpoint_prev_path)
                except Exception as e:
                    logger.warning(f"Error haciendo backup del checkpoint anterior: {e}")
            
            os.rename(temp_path, checkpoint_path)
            
            if self.transactions_logger:
                with open(self.transactions_logger.logger.log_path, 'a') as log_f:
                    log_f.write(f"CHECKPOINT_SAVED:{client_id}:{message_id}\n")
                    log_f.flush()
                    os.fsync(log_f.fileno())
            
        except Exception as e:
            if os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except:
                    pass
            raise
    
    def recover_from_checkpoint(self):
        """
        Recupera estado desde archivos de checkpoint dedicados.
        """
        if not self.transactions_logger:
            logger.warning("No hay logger configurado, saltando recuperación")
            return
        
        logger.info("Recuperando estado desde checkpoints dedicados...")
        
        clients_recovered = 0
        clients_failed = 0
        
        try:
            checkpoint_files = [f for f in os.listdir(self.checkpoint_dir) 
                              if f.startswith('client_') and f.endswith('.checkpoint')]
            
            logger.info(f"Encontrados {len(checkpoint_files)} archivos de checkpoint")
            
            for checkpoint_file in checkpoint_files:
                checkpoint_path = os.path.join(self.checkpoint_dir, checkpoint_file)
                checkpoint_prev_path = f"{checkpoint_path}.prev"
                
                loaded = False
                checkpoint_data = None
                
                try:
                    checkpoint_data = self._load_checkpoint_file(checkpoint_path)
                    loaded = True
                    logger.debug(f"Checkpoint cargado: {checkpoint_file}")
                except Exception as e:
                    logger.warning(f"Error cargando checkpoint principal {checkpoint_file}: {e}")
                
                if not loaded and os.path.exists(checkpoint_prev_path):
                    try:
                        logger.info(f"Intentando cargar backup: {checkpoint_file}.prev")
                        checkpoint_data = self._load_checkpoint_file(checkpoint_prev_path)
                        loaded = True
                        logger.info(f"Checkpoint recuperado desde backup")
                    except Exception as e:
                        logger.error(f"Error cargando backup {checkpoint_file}.prev: {e}")
                
                if loaded and checkpoint_data:
                    try:
                        self._apply_checkpoint(checkpoint_data)
                        clients_recovered += 1
                        logger.info(f"Cliente {checkpoint_data['client_id']} recuperado "
                                  f"(último mensaje: {checkpoint_data['last_message_id']})")
                    except Exception as e:
                        logger.error(f"Error aplicando checkpoint {checkpoint_file}: {e}")
                        clients_failed += 1
                else:
                    clients_failed += 1
        
        except FileNotFoundError:
            logger.info("Directorio de checkpoints no existe - comenzando desde cero")
        except Exception as e:
            logger.error(f"Error durante recuperación: {e}")
        
        logger.info(f"Recuperación completada: {clients_recovered} clientes recuperados, "
                   f"{clients_failed} fallidos")
    
    def _load_checkpoint_file(self, checkpoint_path: str) -> dict:
        """Carga y valida un archivo de checkpoint"""
        with open(checkpoint_path, 'r') as f:
            checkpoint_data = json.load(f)
        
        if not isinstance(checkpoint_data, dict):
            raise ValueError("Checkpoint no es un dict")
        if 'client_id' not in checkpoint_data:
            raise ValueError("Checkpoint sin client_id")
        if 'data' not in checkpoint_data:
            raise ValueError("Checkpoint sin data")
        
        node_type = checkpoint_data.get('node_type')
        if node_type and node_type != self.node.__class__.__name__:
            logger.warning(f"Checkpoint es de tipo {node_type}, esperaba {self.node.__class__.__name__}")

        return checkpoint_data
    
    def _apply_checkpoint(self, checkpoint_data: dict):
        """
        Aplica un checkpoint cargado al estado en memoria.
        """
        client_id = checkpoint_data['client_id']
        data = checkpoint_data['data']
        
        if 'processed_ranges' in checkpoint_data:
            if client_id not in self.message_trackers:
                self.message_trackers[client_id] = MessageRangeTracker()
            
            self.message_trackers[client_id].from_list(checkpoint_data['processed_ranges'])
            
            logger.info(f"Restaurados {len(self.message_trackers[client_id])} rangos "
                    f"({self.message_trackers[client_id].total_messages()} mensajes) "
                    f"para cliente {client_id}")
        
        self.node._deserialize_client_data(client_id, data)
        
    def save_eof_checkpoint(self, client_id: str, message_id: str):
        """
        Guarda checkpoint especial indicando que EOF fue procesado completamente.
        """
        try:
            client_data = self.node._serialize_client_data(client_id)
            
            processed_ranges = []
            total_messages = 0
            if client_id in self.message_trackers:
                processed_ranges = self.message_trackers[client_id].to_list()
                total_messages = self.message_trackers[client_id].total_messages()
            
            checkpoint_data = {
                "client_id": client_id,
                "last_message_id": message_id,
                "processed_ranges": processed_ranges,
                "total_messages_processed": total_messages,
                "eof_processed": True,  
                "eof_message_id": message_id,
                "node_type": self.node.__class__.__name__,
                "data": client_data
            }
            
            self._atomic_write_checkpoint(client_id, message_id, checkpoint_data)
            
            logger.info(f"Checkpoint EOF guardado para {client_id}:{message_id} "
                    f"({total_messages} mensajes procesados)")
            
        except Exception as e:
            logger.error(f"Error guardando checkpoint EOF para {client_id}:{message_id}: {e}")
            raise
