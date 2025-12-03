import json
import logging
import os
import tempfile
from typing import Dict, List
from collections import defaultdict
from message_range_tracker.message_range_tracker import MessageRangeTracker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CheckpointHandler:
    def __init__(self, checkpoint_dir: str = None, strategy = None, checkpont_interval: int = 1000, outgoing_counter_by_client: Dict[str, int] = None, extra_id: int = 0):
        self.strategy = strategy
        self.checkpoint_interval = checkpont_interval
        
        self.checkpoint_dir = checkpoint_dir or '/app/server/logs/checkpoints'
        os.makedirs(self.checkpoint_dir, exist_ok=True)

        self.outgoing_counter_by_client = outgoing_counter_by_client or defaultdict(int)
        self.extra_id = extra_id
        
        self.message_trackers: Dict[str, MessageRangeTracker] = {}
        self.message_count = defaultdict(int)
        
        self.log_files = {}
        
        self.is_first_message = True
        
        logger.info(f"Checkpoint Handler iniciado en {self.checkpoint_dir}")

    def _get_log_path(self, client_id: str) -> str:
        return os.path.join(self.checkpoint_dir, f"client_{client_id}.log")

    def _get_checkpoint_path(self, client_id: str) -> str:
        return os.path.join(self.checkpoint_dir, f"client_{client_id}.checkpoint")

    def _get_or_create_log(self, client_id: str):
        if client_id not in self.log_files:
            log_path = self._get_log_path(client_id)
            self.log_files[client_id] = open(log_path, 'a', encoding='utf-8')
        return self.log_files[client_id]

    def _safe_int(self, value):
        """Convierte a int de forma segura"""
        try:
            return int(value)
        except ValueError:
            return 0

    def save_message_checkpoint(self, client_id: str, message_id: str, csv_lines: list):
        try:
            operations = []
            for line in csv_lines:
                op = self.strategy.serialize_operation(client_id, line)
                if op:
                    operations.append(op)

            msg_id_int = self._safe_int(message_id)
            
            log_entry = {
                "msg_id": msg_id_int,
                "ops": operations
            }
            log_line = json.dumps(log_entry) + "\n"
            
            f = self._get_or_create_log(client_id)
            f.write(log_line)
            f.flush()
            os.fsync(f.fileno()) 
            
            if client_id not in self.message_trackers:
                self.message_trackers[client_id] = MessageRangeTracker()
            
            self.message_trackers[client_id].add_message_id(msg_id_int)
            self.message_count[client_id] += 1
            
            if self.message_count[client_id] % self.checkpoint_interval == 0:
                self._create_snapshot(client_id, msg_id_int)
                
        except Exception as e:
            logger.error(f"FATAL: Error escribiendo checkpoint para {client_id}: {e}")
            raise  

    def save_eof_checkpoint(self, client_id: str, message_id: str):
        """Marca el fin de procesamiento de forma atómica."""
        msg_id_int = self._safe_int(message_id)
        self._create_snapshot(client_id, msg_id_int, is_eof=True)

    def _create_snapshot(self, client_id: str, last_message_id: int, is_eof: bool = False):
        """Vuelca todo el estado a disco y limpia el log."""
        logger.info(f"Creando Snapshot para {client_id} (Last ID: {last_message_id})")
        
        try:
            business_data = self.strategy._serialize_client_data(client_id)
            
            tracker_ranges = []
            if client_id in self.message_trackers:
                tracker_ranges = self.message_trackers[client_id].to_list()
            
            checkpoint_data = {
                "client_id": client_id,
                "last_message_id": last_message_id,
                "processed_ranges": tracker_ranges,
                "eof": is_eof,
                "data": business_data
            }
            
            ckpt_path = self._get_checkpoint_path(client_id)
            temp_path = ckpt_path + ".tmp"
            
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(checkpoint_data, f)
                f.flush()
                os.fsync(f.fileno())
            
            os.replace(temp_path, ckpt_path)
            
            self._truncate_wal(client_id)
            
        except Exception as e:
            logger.error(f"Error creando snapshot para {client_id}: {e}")

    def _truncate_wal(self, client_id: str):
        if client_id in self.log_files:
            self.log_files[client_id].close()
            del self.log_files[client_id]
        
        log_path = self._get_log_path(client_id)
        with open(log_path, 'w') as f:
            pass 
        logger.debug(f"LOG truncado para {client_id}")

    def analyze_first_message(self, client_id: str, message_id: str, ch, method, body) -> bool:
        """Lógica de inicio: Recovery o Deduplicación."""
        
        if self.is_first_message:
            logger.info("INICIO DE NODO: Ejecutando Recovery global...")
            self.recover_from_checkpoint()
            self.is_first_message = False

        msg_id_int = self._safe_int(message_id)
        
        if self._is_processed(client_id, msg_id_int):
            logger.info(f"DUPLICADO DETECTADO: {client_id}:{message_id}. Enviando ACK.")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return True
        
   
        return False 

    def _is_processed(self, client_id: str, msg_id_int: int) -> bool:
        """Consulta el tracker en memoria."""
        if client_id not in self.message_trackers:
            return False
        return self.message_trackers[client_id].contains(msg_id_int)

    def recover_from_checkpoint(self):
        """Busca todos los archivos de clientes y reconstruye el estado."""
        clients = set()
        if not os.path.exists(self.checkpoint_dir):
            return

        for f in os.listdir(self.checkpoint_dir):
            if f.startswith("client_") and (f.endswith(".checkpoint") or f.endswith(".log")):
                parts = f.split('_')
                if len(parts) >= 2:
                    c_id = parts[1].split('.')[0]
                    clients.add(c_id)
        
        logger.info(f"Clientes encontrados para recovery: {clients}")
        
        for client_id in clients:
            self._recover_single_client(client_id)

    def _recover_single_client(self, client_id: str):
            ckpt_path = self._get_checkpoint_path(client_id)
            
            if client_id not in self.message_trackers:
                self.message_trackers[client_id] = MessageRangeTracker()

            if os.path.exists(ckpt_path):
                try:
                    with open(ckpt_path, 'r') as f:
                        data = json.load(f)
                    
                    if 'processed_ranges' in data:
                        self.message_trackers[client_id].from_list(data['processed_ranges'])
                    
                    if 'data' in data:
                        self.strategy._deserialize_client_data(client_id, data['data'])
                    
                    logger.info(f"Cliente {client_id}: Checkpoint cargado.")
                    
                except Exception as e:
                    logger.error(f"Cliente {client_id}: Checkpoint corrupto o ilegible: {e}")
            
            log_path = self._get_log_path(client_id)
            if os.path.exists(log_path):
                logger.info(f"Cliente {client_id}: Replaying LOG...")
                ops_applied = 0
                
                with open(log_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line: continue
                        try:
                            entry = json.loads(line)
                            msg_id = entry['msg_id']
                            if not self.message_trackers[client_id].contains(msg_id):
                                ops = entry['ops']
                                for op_str in ops:
                                    op_dict = self.strategy.deserialize_operation(op_str)
                                    self.strategy.apply_operation(op_dict)
                                
                                self.message_trackers[client_id].add_message_id(msg_id)
                                ops_applied += 1
                            else:
                                pass

                        except Exception as e:
                            logger.warning(f"Línea corrupta en LOG de {client_id}: {e}")
                
                logger.info(f"Cliente {client_id}: Replay finalizado. {ops_applied} mensajes recuperados del log.")


    def save_counter_update(self, client_id: str, counter_value : int, transaction : str):
        """
        Guarda la actualización del contador en el LOG.
        """
        try:
            log_entry = {
                "msg_id": counter_value,
                "ops": transaction
            }
            log_line = json.dumps(log_entry) + "\n"
            
            f = self._get_or_create_log(client_id)
            f.write(log_line)
            f.flush()
            os.fsync(f.fileno()) 
            
        except Exception as e:
            logger.error(f"FATAL: Error guardando contador para {client_id}: {e}")
            raise
        
    def start_batch_transaction(self, client_id: str, query_name: str):
        current_counter = self.outgoing_counter_by_client.get(client_id, 0)
        
        op = f"{client_id},START_TRANSACTION,{current_counter},{query_name}"
        self.save_counter_update(client_id, current_counter, [op])

    def get_next_id_in_memory(self, client_id: str) -> int:
        self.outgoing_counter_by_client[client_id] += 1
        return int(self.extra_id) * 1_000_000 + self.outgoing_counter_by_client[client_id]

    def commit_batch_transaction(self, client_id: str, query_name: str):
        current_counter = self.outgoing_counter_by_client.get(client_id, 0)
        
        ops = [
            f"{client_id},COMMIT_TRANSACTION,{current_counter}",
            f"{client_id},SENT,{query_name}"
        ]
        self.save_counter_update(client_id, current_counter, ops)
    
    def close(self):
        for f in self.log_files.values():
            try:
                f.close()
            except:
                pass
        self.log_files.clear()