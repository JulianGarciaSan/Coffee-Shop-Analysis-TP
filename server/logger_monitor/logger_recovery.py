import logging
from typing import Optional, NamedTuple
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


class RecoveryAction(NamedTuple):
    """Info de la acción a tomar"""
    state: RecoveryState
    client_id: Optional[str] = None
    batch_type: Optional[str] = None


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
    
    def check_startup_recovery(self) -> RecoveryAction:
        """
        Analiza logs al iniciar.
        Retorna qué acción tomar antes de procesar mensajes.
        """
        last_line = self.eof_logger._get_last_line()
        
        if not last_line or self.LOG_DELIMITER not in last_line:
            return RecoveryAction(RecoveryState.PROCESS_NEW)
        
        parts = last_line.split(self.LOG_SPLITTER, 2)
        status = parts[0] if len(parts) > 0 else ""
        client_id = parts[1] if len(parts) > 1 else ""
        batch_type = parts[2] if len(parts) > 2 else ""
        
        # EOF pendiente: completar
        if "EOF" in status:
            return RecoveryAction(
                RecoveryState.COMPLETE_EOF,
                client_id=client_id,
                batch_type=batch_type
            )
        
        # BEF pendiente: reenviar
        if "BEF" in status:
            return RecoveryAction(
                RecoveryState.RESEND_EOF,
                client_id=client_id
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
        
        # Parse client log: "client_id;message_id"
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
        """Parse: client_id;message_id"""
        if not line or self.LOG_DELIMITER not in line:
            return None, None
        try:
            cid, mid = line.split(self.LOG_SPLITTER)
            return cid, mid
        except:
            return None, None
    
    def _parse_eof_log(self, line: str) -> tuple:
        """Parse: STATUS:client_id:batch_type"""
        if not line or self.LOG_DELIMITER not in line:
            return None, None, None
        try:
            parts = line.split(self.LOG_SPLITTER)
            if len(parts) >= 3:
                return parts[0], parts[1], parts[2]
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
                client_id=eof_cid
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