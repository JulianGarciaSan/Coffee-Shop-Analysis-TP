import logging
from typing import Optional, NamedTuple, Dict, Set, List, Tuple
from enum import Enum
from logger_monitor.logger_monitor import LoggerMonitor

logger = logging.getLogger(__name__)


class RecoveryState(Enum):
    """Estados posibles de recuperación"""
    PROCESS_NEW = "process_new"              # Procesar mensaje nuevo
    ALREADY_DONE = "already_done"            # Ya procesado, solo ACK
    COMPLETE_EOF = "complete_eof"            # Completar EOF pendiente
    RESEND_EOF = "resend_eof"                # Reenviar EOF
    RETRY_SEND = "retry_send"                # Reintentar envío
    MULTIPLE_PENDING = "multiple_pending"    # Múltiples clientes pendientes


class RecoveryAction(NamedTuple):
    """Info de la acción a tomar"""
    state: RecoveryState
    client_id: Optional[str] = None
    batch_type: Optional[str] = None
    pending_clients: Optional[List[tuple]] = None  # Lista de (client_id, batch_type, is_eof)


class ClientState:
    """Estado de un cliente según los logs"""
    def __init__(self, client_id: str):
        self.client_id = client_id
        self.has_bef = False
        self.has_eof = False
        self.has_end = False
        self.batch_type: Optional[str] = None
        self.last_status: Optional[str] = None  # "BEF", "EOF", o "END"
    
    def is_pending(self) -> bool:
        """Retorna True si el cliente tiene trabajo pendiente"""
        return (self.has_bef or self.has_eof) and not self.has_end
    
    def needs_eof_completion(self) -> bool:
        """Retorna True si tiene EOF pero no END"""
        return self.last_status == "EOF" and not self.has_end
    
    def needs_eof_resend(self) -> bool:
        """Retorna True si tiene BEF pero no EOF ni END"""
        return self.last_status == "BEF" and not self.has_eof and not self.has_end


class RecoveryManager:
    """
    Analiza logs y retorna QUÉ hacer, no LO HACE.
    """
    
    def __init__(
        self,
        main_logger: LoggerMonitor,
        client_logger: LoggerMonitor,
        eof_logger: LoggerMonitor
    ):
        self.main_logger = main_logger
        self.client_logger = client_logger
        self.eof_logger = eof_logger
        
        self.LOG_DELIMITER = ';'
        self.LOG_SPLITTER = ':'
    
    def load_dedup_state(self) -> Tuple[Set[Tuple[int, int]], Set[int]]:
        """
        NUEVO MÉTODO: Carga el estado de deduplicación desde UN SOLO LOG.
        
        El log contiene todas las entradas en formato: CLIENT_ID:MSG_ID;
        
        Los clientes finalizados se detectan porque sus EOFs también son mensajes
        que vienen con el payload "EOF:X", pero en el log solo guardamos client_id:message_id.
        
        Para saber qué clientes finalizaron, necesitamos trackear qué client_ids
        procesamos sus EOFs. Esto lo hacemos en memoria en el FilterDuplicateNode,
        acá solo cargamos los mensajes procesados.
        
        Returns:
            Tuple de (processed_messages, eof_clients)
            - processed_messages: Set[(client_id, message_id)]
            - eof_clients: Set[client_id] - VACÍO porque no lo guardamos en log
        """
        processed_messages: Set[Tuple[int, int]] = set()
        eof_clients: Set[int] = set()  # Siempre vacío, se reconstruye en runtime
        
        # Cargar mensajes procesados desde dedup_log
        try:
            with open(self.main_logger.log_path, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    
                    # Verificar delimitador
                    if not line.endswith(self.LOG_DELIMITER):
                        logger.warning(f"Dedup log línea {line_num}: sin delimitador, ignorando")
                        continue
                    
                    # Remover delimitador
                    line = line[:-1]
                    
                    # Parse: CLIENT_ID:MSG_ID
                    try:
                        parts = line.split(self.LOG_SPLITTER)
                        if len(parts) == 2:
                            client_id = int(parts[0])
                            message_id = int(parts[1])
                            processed_messages.add((client_id, message_id))
                        else:
                            logger.warning(f"Dedup log línea {line_num}: formato inválido: {line}")
                    except ValueError as e:
                        logger.warning(f"Dedup log línea {line_num}: error parseando: {e}")
        
        except FileNotFoundError:
            logger.info("No existe dedup_log.txt, comenzando desde cero")
        except Exception as e:
            logger.error(f"Error cargando dedup_log: {e}")
        
        logger.info(f"Dedup state cargado: {len(processed_messages)} mensajes procesados")
        
        # NOTA: eof_clients se reconstruirá en runtime cuando lleguen nuevos mensajes
        # Si un cliente ya envió EOF y crasheamos, al reiniciar:
        # 1. El EOF está en processed_messages como (client_id, eof_message_id)
        # 2. Si llega un mensaje normal de ese cliente, NO está en eof_clients
        # 3. Lo procesaremos y enviaremos (duplicado aceptable)
        # 4. Cuando llegue el EOF de nuevo (duplicado), lo detectaremos y no lo procesaremos
        
        return processed_messages, eof_clients
    
    def _parse_all_eof_logs(self) -> Dict[str, ClientState]:
        """
        Lee TODO el archivo eof_logs.txt y construye el estado de cada cliente.
        Retorna un diccionario {client_id: ClientState}
        """
        client_states: Dict[str, ClientState] = {}
        
        try:
            with open(self.eof_logger.log_path, 'r') as f:
                lines = f.readlines()
        except FileNotFoundError:
            logger.info("No hay archivo de EOF logs, inicio limpio")
            return client_states
        except Exception as e:
            logger.error(f"Error leyendo EOF logs: {e}")
            return client_states
        
        for line in lines:
            line = line.strip()
            if not line or self.LOG_DELIMITER not in line:
                continue
            
            # Parse: STATUS:client_id:batch_type;
            # Ejemplo: BEF:2:transactions;
            parts = line.split(self.LOG_SPLITTER)
            if len(parts) < 2:
                continue
            
            status = parts[0]
            client_id = parts[1]
            batch_type = parts[2].rstrip(self.LOG_DELIMITER) if len(parts) > 2 else None
            
            # Inicializar estado si no existe
            if client_id not in client_states:
                client_states[client_id] = ClientState(client_id)
            
            # Actualizar estado según el log
            if status == "BEF":
                client_states[client_id].has_bef = True
                client_states[client_id].last_status = "BEF"
                if batch_type:
                    client_states[client_id].batch_type = batch_type
            
            elif status == "EOF":
                client_states[client_id].has_eof = True
                client_states[client_id].last_status = "EOF"
                if batch_type:
                    client_states[client_id].batch_type = batch_type
            
            elif status == "END":
                client_states[client_id].has_end = True
                client_states[client_id].last_status = "END"
        
        return client_states
    
    def check_startup_recovery(self) -> RecoveryAction:
        """
        Analiza logs al iniciar.
        Retorna qué acción tomar antes de procesar mensajes.
        
        Ahora verifica TODOS los clientes, no solo el último.
        """
        client_states = self._parse_all_eof_logs()
        
        if not client_states:
            return RecoveryAction(RecoveryState.PROCESS_NEW)
        
        pending_eof_completions = [] 
        pending_eof_resends = []      
        
        for client_id, state in client_states.items():
            if state.needs_eof_completion():
                pending_eof_completions.append((client_id, state.batch_type, True))
                logger.warning(f"Cliente {client_id} quedó en EOF sin END")
            
            elif state.needs_eof_resend():
                pending_eof_resends.append((client_id, state.batch_type, False))
                logger.warning(f"Cliente {client_id} quedó en BEF sin EOF")
        
        all_pending = pending_eof_completions + pending_eof_resends
        
        if all_pending:
            if len(all_pending) == 1:
                client_id, batch_type, is_eof = all_pending[0]
                if is_eof:
                    logger.info(f"Completando EOF pendiente para cliente {client_id}")
                    return RecoveryAction(
                        RecoveryState.COMPLETE_EOF,
                        client_id=client_id,
                        batch_type=batch_type
                    )
                else:
                    logger.info(f"Reenviando EOF para cliente {client_id}")
                    return RecoveryAction(
                        RecoveryState.RESEND_EOF,
                        client_id=client_id,
                        batch_type=batch_type
                    )
            else:
                logger.warning(
                    f"Múltiples clientes pendientes: "
                    f"{len(pending_eof_completions)} EOFs, {len(pending_eof_resends)} BEFs"
                )
                return RecoveryAction(
                    RecoveryState.MULTIPLE_PENDING,
                    pending_clients=all_pending
                )
        
        return RecoveryAction(RecoveryState.PROCESS_NEW)
    
    def check_message_recovery(
        self,
        main_log: str,
        current_client_id: Optional[int],
        current_message_id: Optional[int]
    ) -> RecoveryAction:
        """
        Analiza si un mensaje ya fue procesado.
        Retorna qué hacer con este mensaje.
        """
        # Leer logs
        client_log = self.client_logger._get_last_line()
        eof_log = self.eof_logger._get_last_line()
        
        # Convertir IDs a string
        curr_cid = str(current_client_id) if current_client_id else ""
        curr_mid = str(current_message_id) if current_message_id else ""
        
        # Parse client log: "client_id:message_id"
        log_cid, log_mid = self._parse_client_log(client_log)
        
        # Parse EOF log: "STATUS:client_id:batch_type"
        eof_status, eof_cid, eof_batch = self._parse_eof_log(eof_log)
        
        # Analizar según estado del main_log
        if "END" in main_log:
            return self._analyze_end_state(
                curr_cid, curr_mid, log_cid, log_mid,
                eof_status, eof_cid, eof_batch
            )
        
        if "EOF" in main_log:
            return self._analyze_eof_state(
                curr_cid, curr_mid, log_cid, log_mid,
                eof_status, eof_cid, eof_batch
            )
        
        if "E" in main_log:  # Encolado
            if log_cid == curr_cid and log_mid == curr_mid:
                return RecoveryAction(RecoveryState.ALREADY_DONE)
        
        if "F" in main_log:  # Filtrado
            return RecoveryAction(RecoveryState.RETRY_SEND)
        
        return RecoveryAction(RecoveryState.PROCESS_NEW)
    
    def _parse_client_log(self, line: str) -> tuple:
        """Parse: client_id:message_id"""
        if not line or self.LOG_DELIMITER not in line:
            return None, None
        try:
            cid, mid = line.split(self.LOG_SPLITTER)
            return cid, mid
        except:
            return None, None
    
    def _parse_eof_log(self, line: str) -> tuple:
        """Parse: STATUS:client_id:batch_type;"""
        if not line or self.LOG_DELIMITER not in line:
            return None, None, None
        try:
            # Remover el delimitador final
            line = line.rstrip(self.LOG_DELIMITER)
            parts = line.split(self.LOG_SPLITTER)
            if len(parts) >= 3:
                return parts[0], parts[1], parts[2]
            elif len(parts) == 2:
                return parts[0], parts[1], None
        except:
            pass
        return None, None, None
    
    def _analyze_end_state(
        self, curr_cid, curr_mid, log_cid, log_mid,
        eof_status, eof_cid, eof_batch
    ) -> RecoveryAction:
        """Analiza cuando main_log tiene END"""
        
        # Si eof_log también está en END y coincide el mensaje
        if eof_status == "END":
            if curr_cid == log_cid and curr_mid == log_mid:
                return RecoveryAction(RecoveryState.ALREADY_DONE)
            return RecoveryAction(RecoveryState.PROCESS_NEW)
        
        # Si eof_log está en EOF, hay que completarlo
        if eof_status == "EOF":
            # Primero completar EOF pendiente
            action = RecoveryAction(
                RecoveryState.COMPLETE_EOF,
                client_id=eof_cid,
                batch_type=eof_batch
            )
            # Después ver si el mensaje actual ya estaba procesado
            if curr_cid == log_cid and curr_mid == log_mid:
                return RecoveryAction(RecoveryState.ALREADY_DONE)
            return action
        
        # Si eof_log está en BEF, hay que reenviarlo
        if eof_status == "BEF":
            return RecoveryAction(
                RecoveryState.RESEND_EOF,
                client_id=eof_cid,
                batch_type=eof_batch
            )
        
        return RecoveryAction(RecoveryState.PROCESS_NEW)
    
    def _analyze_eof_state(
        self, curr_cid, curr_mid, log_cid, log_mid,
        eof_status, eof_cid, eof_batch
    ) -> RecoveryAction:
        """Analiza cuando main_log tiene EOF"""
        
        # Si eof_log está en END: ya llegaron todos los ACKs
        # Solo falta ACK del primer EOF
        if eof_status == "END":
            if log_cid == eof_cid == curr_cid and log_mid == curr_mid:
                return RecoveryAction(RecoveryState.ALREADY_DONE)
            return RecoveryAction(RecoveryState.PROCESS_NEW)
        
        # Si eof_log está en EOF, completarlo
        if eof_status == "EOF":
            # Completar EOF pendiente primero
            if eof_cid and eof_batch:
                # Si es el mismo mensaje, ya está procesado
                if log_cid == eof_cid == curr_cid and log_mid == curr_mid:
                    return RecoveryAction(RecoveryState.ALREADY_DONE)
                # Sino, completar EOF
                return RecoveryAction(
                    RecoveryState.COMPLETE_EOF,
                    client_id=eof_cid,
                    batch_type=eof_batch
                )
        
        return RecoveryAction(RecoveryState.PROCESS_NEW)