import json
import logging
import os
import tempfile
from typing import Dict
from collections import defaultdict
from message_range_tracker.message_range_tracker import MessageRangeTracker
from logger_monitor.logger_writter import LogWriter


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CheckpointHandler:
    def __init__(self, checkpoint_dir: str = None, strategy = None, checkpont_interval: int = 1000):
        self.message_trackers: Dict[str, MessageRangeTracker] = {}
        self.strategy = strategy
        # self.client_logger = LogWriter('/app/client_logs.txt')
        self.is_first_message = True

        if checkpoint_dir:
            self.checkpoint_dir = checkpoint_dir
        else:
            self.checkpoint_dir = '/app/server/logs/checkpoints'
        
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        
        self.log_files = {}  
        self.message_count = defaultdict(int)  
        
        self.fsync_interval = 100 
        self.checkpoint_interval = checkpont_interval
        
        logger.info(f"Sistema de checkpoints inicializado en {self.checkpoint_dir}")

    def _get_log_path(self, client_id: str) -> str:
        """Obtiene la ruta del archivo LOG para un cliente."""
        return os.path.join(self.checkpoint_dir, f"client_{client_id}.log")

    def _get_or_create_log(self, client_id: str):
        """Obtiene o crea el archivo LOG para un cliente."""
        if client_id not in self.log_files:
            log_path = self._get_log_path(client_id)
            self.log_files[client_id] = open(log_path, 'a', buffering=1)
            logger.debug(f"LOG abierto: {log_path}")
        return self.log_files[client_id]

    def save_message_checkpoint(self, client_id: str, message_id: str, csv_lines: list):
        """
        Guarda checkpoint para un mensaje en una sola línea del log.
        Formato: MSG:message_id:op1|op2|op3|...
        """
        try:
            if not hasattr(self.strategy, 'serialize_operation'):
                raise AttributeError(
                    f"Strategy {type(self.strategy).__name__} no tiene método 'serialize_operation'"
                )
            
            operations = []
            skipped_lines = 0
            
            for csv_line in csv_lines:
                operation_str = self.strategy.serialize_operation(client_id, csv_line)
                
                if operation_str is not None:
                    operations.append(operation_str)
                else:
                    skipped_lines += 1
            
            if not operations:
                return
            
            batch_operation = '|'.join(operations)
            
            log_file = self._get_or_create_log(client_id)
            log_file.write(f"MSG:{message_id}:{batch_operation}\n")
            
            if client_id not in self.message_trackers:
                self.message_trackers[client_id] = MessageRangeTracker()
            
            try:
                if hasattr(self.strategy, '_message_id_to_int'):
                    msg_id_int = self.strategy._message_id_to_int(message_id)
                else:
                    # Fallback para estrategias sin este método
                    if '_' in message_id:
                        msg_id_int = int(message_id.split('_')[-1])
                    else:
                        msg_id_int = int(message_id)
            except ValueError as e:
                return
            
            self.message_trackers[client_id].add_message_id(msg_id_int)
            logger.debug(f"Mensaje {message_id} marcado como completo")
            
            self.message_count[client_id] += 1
            
            if self.message_count[client_id] % self.fsync_interval == 0:
                log_file.flush()
                os.fsync(log_file.fileno())
                logger.debug(f"Fsync realizado para cliente {client_id}")
            
            if self.message_count[client_id] % self.checkpoint_interval == 0:
                self._create_snapshot(client_id, message_id)
            
        except Exception as e:
            logger.error(f"Error guardando checkpoint de batch: {e}")
            raise
    
    def _create_snapshot(self, client_id: str, message_id: str):
        """
        Crea snapshot completo del estado en memoria.
        """
        logger.info(f"Creando snapshot para cliente {client_id} en mensaje {message_id}")
        
        try:
            client_data = self.strategy._serialize_client_data(client_id)
            
            checkpoint_data = {
                "client_id": client_id,
                "last_message_id": message_id,
                "processed_ranges": self.message_trackers[client_id].to_list(),
                "strategy_type": type(self.strategy).__name__,
                "data": client_data
            }
            
            self._atomic_write_checkpoint(client_id, message_id, checkpoint_data)
            
            # self._truncate_wal(client_id)
            
            logger.info(f"Snapshot completado para cliente {client_id}, LOG truncado")
            
        except Exception as e:
            logger.error(f"Error creando snapshot para {client_id}: {e}")
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
            
        except Exception as e:
            if os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except:
                    pass
            raise
    
    def recover_from_checkpoint(self):
        """
        Recupera estado desde checkpoints + replay de LOG.
        """
        
        logger.info("Iniciando recovery desde checkpoints + LOG...")
        
        clients_recovered = 0
        clients_failed = 0
        
        clients = self._find_clients()
        logger.info(f"Encontrados {len(clients)} clientes para recuperar")
        
        for client_id in clients:
            try:
                self._recover_client(client_id)
                clients_recovered += 1
            except Exception as e:
                logger.error(f"Error recuperando cliente {client_id}: {e}")
                clients_failed += 1
        
        logger.info(f"Recovery completado: {clients_recovered} clientes recuperados, {clients_failed} fallidos")
    
    def _find_clients(self) -> list:
        """
        Encuentra todos los clientes con checkpoints o logs.
        """
        clients = set()
        
        try:
            for filename in os.listdir(self.checkpoint_dir):
                if filename.startswith('client_') and '.checkpoint' in filename:
                    client_id = filename.split('_')[1].split('.')[0]
                    clients.add(client_id)
                elif filename.endswith('.log'):
                    client_id = filename.split('_')[1].split('.')[0]
                    clients.add(client_id)
        except FileNotFoundError:
            logger.info("Directorio de checkpoints no existe - comenzando desde cero")
        
        return list(clients)
    
    def _recover_client(self, client_id: str):
        """
        Recupera estado de un cliente específico: checkpoint + LOG replay.
        """
        logger.info(f"Recuperando cliente {client_id}...")
        
        checkpoint_data = self._load_checkpoint(client_id)
        
        if checkpoint_data:
            self._apply_checkpoint(checkpoint_data)
            last_checkpoint_id = int(checkpoint_data['last_message_id'])
            logger.info(f"Checkpoint cargado hasta mensaje {last_checkpoint_id}")
        else:
            last_checkpoint_id = 0
            logger.info(f"No hay checkpoint para cliente {client_id}, empezando desde cero")
        
        log_path = self._get_log_path(client_id)
        
        if os.path.exists(log_path):
            replayed = self._replay_log(client_id, log_path, last_checkpoint_id)
            logger.info(f"Replay completado: {replayed} operaciones aplicadas desde el log")
        else:
            logger.info(f"No hay log para cliente {client_id}")

    def _load_checkpoint(self, client_id: str) -> dict:
        """
        Carga checkpoint con fallback a backup (.prev).
        """
        checkpoint_path = os.path.join(self.checkpoint_dir, f"client_{client_id}.checkpoint")
        checkpoint_prev_path = f"{checkpoint_path}.prev"
        
        if os.path.exists(checkpoint_path):
            try:
                with open(checkpoint_path, 'r') as f:
                    checkpoint_data = json.load(f)
                logger.debug(f"Checkpoint principal cargado para cliente {client_id}")
                return checkpoint_data
            except Exception as e:
                logger.warning(f"Error cargando checkpoint principal: {e}")
        
        if os.path.exists(checkpoint_prev_path):
            try:
                with open(checkpoint_prev_path, 'r') as f:
                    checkpoint_data = json.load(f)
                logger.info(f"Checkpoint recuperado desde BACKUP (.prev) para cliente {client_id}")
                return checkpoint_data
            except Exception as e:
                logger.error(f"Error cargando backup: {e}")
        
        return None
    
    def _replay_log(self, client_id: str, log_path: str, last_checkpoint_id: int) -> int:
        """
        Replay de operaciones desde el log
        """
        replayed_count = 0
        
        logger.info(f"Iniciando replay de logs para cliente {client_id} desde mensaje {last_checkpoint_id + 1}")
        
        with open(log_path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                
                line = line.rstrip('\n')  
                
                if not line:
                    continue
                
                if not line.startswith('MSG:'):
                    logger.warning(f"Línea {line_num} con formato desconocido: {line[:50]}...")
                    continue
                
                parts = line.split(':', 2)
                
                if len(parts) != 3:
                    logger.warning(f"Línea {line_num} incompleta o corrupta")
                    logger.warning(f"Deteniendo replay - posible fallo durante escritura")
                    break
                
                _, message_id, batch_operation = parts
                
                try:
                    msg_id_int = int(message_id)
                    
                    if msg_id_int <= last_checkpoint_id:
                        logger.debug(f"Saltando mensaje {msg_id_int} (ya en checkpoint)")
                        continue
                    
                    operations_str = batch_operation.split('|')
                    
                    for op_str in operations_str:
                        if not op_str.strip():
                            continue
                        
                        operation = self.strategy.deserialize_operation(op_str)
                        self.strategy.apply_operation(operation)
                    
                    if client_id not in self.message_trackers:
                        self.message_trackers[client_id] = MessageRangeTracker()
                    self.message_trackers[client_id].add_message_id(msg_id_int)
                    
                    replayed_count += 1
                    
                    if replayed_count % 100 == 0:
                        logger.debug(f" Replay progreso: {replayed_count} mensajes")
                    
                except ValueError as e:
                    logger.error(f"Error parseando message_id en línea {line_num}: {e}")
                    logger.warning(f"Deteniendo replay")
                    break
                except Exception as e:
                    logger.error(f"Error en línea {line_num}: {e}")
                    logger.warning(f"   Línea: {line[:100]}...")
                    logger.warning(f"Deteniendo replay")
                    break
        
        return replayed_count
    
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
        
        if not hasattr(self.strategy, '_deserialize_client_data'):
            raise AttributeError(
                f"Strategy {type(self.strategy).__name__} no tiene método '_deserialize_client_data'"
            )
        
        self.strategy._deserialize_client_data(client_id, data)
    
    def save_eof_checkpoint(self, client_id: str, message_id: str):
        """
        Guarda checkpoint especial indicando que EOF fue procesado completamente.
        """
        try:
            client_data = self.strategy._serialize_client_data(client_id)
            
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
                "strategy_type": type(self.strategy).__name__,
                "data": client_data
            }
            
            self._atomic_write_checkpoint(client_id, message_id, checkpoint_data)
            
            # if client_id in self.log_files:
            #     self._truncate_wal(client_id)
            
            logger.info(f"Checkpoint EOF guardado para {client_id}:{message_id}")
            
        except Exception as e:
            logger.error(f"Error guardando checkpoint EOF para {client_id}:{message_id}: {e}")
            raise
        
       
    def analyze_first_message(self, client_id: str, message_id: str, ch, method, body) -> bool:
        """
        Versión mejorada: distingue recovery (primer mensaje) vs duplicación (nodo corriendo).
        """
        if self.is_first_message:
            logger.info(f"RECOVERY: {client_id}:{message_id}")
            self.is_first_message = False
            
            try:
                self.recover_from_checkpoint()
                
                if client_id in self.message_trackers:
                    total = self.message_trackers[client_id].total_messages()
                    logger.info(f"Recuperados {total} mensajes desde checkpoint+LOG")

            except Exception as e:
                logger.error(f"Error en recovery: {e}")
            
            if client_id in self.message_trackers:
                tracker = self.message_trackers[client_id]
                tracker = self.message_trackers[client_id]
                logger.info(f"Actual:{client_id}:{message_id} ")
                logger.info(f"Rango mensajes procesados {client_id}: {tracker.ranges}")
            if self._eof_already_processed(client_id):
                logger.info(f"EOF COMPLETADO en checkpoint - ENVIANDO ACK")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return True
            
            if self._message_in_checkpoint(client_id, message_id):
                logger.info(f"Mensaje {message_id} YA en checkpoint - SKIPPING Y ACK")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return True
            logger.info(f" MENSAJE NO PROCESADO - REPROCESANDO")
            return False
        
        if self._message_in_tracker(client_id, message_id):
            logger.info(f"DUPLICADO detectado: {client_id}:{message_id} (nodo corriendo) - SKIPPING")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return True
        
        return False

    def _message_in_checkpoint(self, client_id: str, message_id: str) -> bool:
        """
        Verifica si el mensaje específico fue procesado usando rangos.
        """
        try:
            if client_id not in self.message_trackers:
                return False
                
            msg_id_int = self._extract_message_id_int(message_id)
            is_processed = self.message_trackers[client_id].contains(msg_id_int)
            
            if is_processed:
                logger.debug(f"Mensaje {client_id}:{message_id} encontrado en rangos procesados")
            
            return is_processed
            
        except Exception as e:
            logger.warning(f"Error verificando mensaje en rangos: {e}")
            return False
    
    def _message_in_tracker(self, client_id: str, message_id: str) -> bool:
        """
        Verifica si mensaje está en tracker EN MEMORIA (para nodo corriendo).
        """
        try:
            if client_id not in self.message_trackers:
                return False
            
            msg_id_int = self._extract_message_id_int(message_id)
            return self.message_trackers[client_id].contains(msg_id_int)
            
        except Exception as e:
            logger.warning(f"Error verificando tracker: {e}")
            return False
        
    def _extract_message_id_int(self, message_id: str) -> int:
        """
        Extrae ID numérico del message_id.
        Soporta: "8273", "0_8273", "8273:A0", "8273:EOF"
        """
        try:
            # Remover sufijos (:A0, :EOF, :TC)
            base_id = message_id.split(':')[0]
            
            # Remover prefijo de cliente (0_8273 → 8273)
            if '_' in base_id:
                base_id = base_id.split('_')[-1]
            
            return int(base_id)
            
        except Exception as e:
            logger.error(f"Error extrayendo ID de '{message_id}': {e}")
            # if hasattr(self.strategy, '_message_id_to_int'):
            #     return self.strategy._message_id_to_int(message_id)
            raise
    
        
        
    def _eof_already_processed(self, client_id: str) -> bool:
        """
        Verifica si el EOF de un cliente ya fue procesado completamente.
        """
        
        checkpoint_path = os.path.join(
            self.checkpoint_dir,
            f"client_{client_id}.checkpoint"
        )
        
        if not os.path.exists(checkpoint_path):
            return False
        
        try:
            with open(checkpoint_path, 'r') as f:
                checkpoint_data = json.load(f)
            
            eof_processed = checkpoint_data.get('eof_processed', False)
            
            if eof_processed:
                logger.info(f"EOF de cliente {client_id} ya procesado según checkpoint")
                return True
            
        except Exception as e:
            logger.warning(f"Error leyendo checkpoint para verificar EOF: {e}")
        
        return False
    
    # def register_incoming_message(self, client_id: str, message_id: str):
    #     self.client_logger.write(f"{client_id};{message_id}")
    
    def close(self):
        """
        Cierra todos los archivos.
        """
        logger.info("Cerrando archivos de logs...")
        for client_id, log_file in self.log_files.items():
            try:
                log_file.close()
                logger.debug(f"Log cerrado para cliente {client_id}")
            except Exception as e:
                logger.warning(f"Error cerrando log para cliente {client_id}: {e}")

        self.log_files.clear()
        
        
    # def _truncate_wal(self, client_id: str):
    #     """
    #     Vacía el LOG después de un checkpoint exitoso.
    #     """
    #     if client_id in self.log_files:
    #         try:
    #             self.log_files[client_id].close()
                
    #             wal_path = self._get_wal_path(client_id)
    #             with open(wal_path, 'w') as f:
    #                 pass  
                
    #             self.log_files[client_id] = open(wal_path, 'a', buffering=1)
                
    #             logger.debug(f"WAL truncado para cliente {client_id}")
                
    #         except Exception as e:
    #             logger.error(f"Error truncando WAL para cliente {client_id}: {e}")
    #             raise