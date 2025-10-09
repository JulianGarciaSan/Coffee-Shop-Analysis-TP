from dataclasses import dataclass
import struct
import socket
import logging
from typing import Optional

logger = logging.getLogger(__name__)

@dataclass
class ProtocolMessage:
    """Representa un mensaje parseado del protocolo."""
    action: str           # BATCH, FINISH, EXIT
    file_type: str        # D, U, S, RQ1, RQ3, RQ4
    last_batch: bool      # True/False
    data: str             # Contenido CSV
    size: int             # Tamaño del payload original

class ProtocolNew:
    HEADER_SIZE = 4  # Tamaño del header en bytes (uint32 BigEndian)

    def __init__(self, socket_conn: socket.socket):
        self.socket_conn = socket_conn

    # ============== ENVÍO DE MENSAJES ==============
    
    def send_batch_message(self, batch_result, file_type: str) -> bool:
        """Envía un batch de datos al servidor."""
        try:
            logger.debug(f"BatchResult type: {type(batch_result)}")
            logger.debug(f"BatchResult attributes: {dir(batch_result)}")
        
            # CORREGIR: Extraer solo el contenido CSV
            if hasattr(batch_result, 'data') and batch_result.data:
                csv_content = batch_result.data  # Usar .data si existe
            elif hasattr(batch_result, 'items') and batch_result.items:
                # Si no tiene .data, unir los items con \n
                csv_content = '\n'.join(batch_result.items)
            else:
                logger.error(f"BatchResult no tiene data ni items válidos")
                return False
            
            # Verificar que sea un string de CSV, no el objeto
            if csv_content.startswith('BatchResult('):
                logger.error(f"ERROR: Enviando objeto BatchResult en lugar de CSV")
                return False
            
            is_last_batch = batch_result.is_last_batch if hasattr(batch_result, 'is_last_batch') else False
            
            # Construir mensaje: BATCH|{file_type}|{last_batch}|{csv_content}
            last_batch_flag = "1" if is_last_batch else "0"
            message = f"BATCH|{file_type}|{last_batch_flag}|{csv_content}"
            
            success = self._send_message(message)
            if not success:
                return False
            
            # Esperar ACK
            ack = self._receive_message()
            if ack != "OK":
                logger.error(f"ACK inválido recibido: {ack}")
                return False
            
            # Contar items correctamente
            items_count = len(batch_result.items) if hasattr(batch_result, 'items') else csv_content.count('\n')
            # logger.info(f"Batch enviado y confirmado: {items_count} items, FILE-TYPE: {file_type}, LAST-BATCH: {is_last_batch}")
            return True
            
        except Exception as e:
            logger.error(f"Error enviando batch: {e}")
            return False

    def send_finish_message(self, file_type: str) -> bool:
        """Envía mensaje FINISH al servidor."""
        try:
            message = f"FINISH|{file_type}"
            success = self._send_message(message)
            if not success:
                return False
            
            # Esperar ACK
            ack = self._receive_message()
            if ack != "OK":
                logger.error(f"ACK inválido para FINISH: {ack}")
                return False
            
            logger.info(f"Mensaje FINISH enviado y confirmado para file_type: {file_type}")
            return True
            
        except Exception as e:
            logger.error(f"Error enviando FINISH: {e}")
            return False

    def send_exit_message(self) -> bool:
        """Envía mensaje EXIT al servidor."""
        try:
            success = self._send_message("EXIT")
            if not success:
                return False
            
            # Esperar ACK
            ack = self._receive_message()
            if ack != "OK":
                logger.error(f"ACK inválido para EXIT: {ack}")
                return False
            
            logger.info("Mensaje EXIT enviado y confirmado")
            return True
            
        except Exception as e:
            logger.error(f"Error enviando EXIT: {e}")
            return False
        
    def send_response_batches(self, action: str, file_type: str, csv_content: str) -> bool:
        """Envía datos en múltiples batches como respuesta (método de compatibilidad)."""
        try:
            # Extraer query_id del action
            # Ej: "RPRT_Q1" -> query_id = "1"
            if action.startswith("RPRT_Q"):
                query_id = action[5:]  # Extraer número después de "RPRT_Q"
            elif action.startswith("RPRT_"):
                query_id = action[5:]  # Extraer parte después de "RPRT_"
            else:
                query_id = "1"  # Default
            
            # Usar el método que ya existe
            return self.send_complete_report(csv_content, query_id, 50000)
            
        except Exception as e:
            logger.error(f"Error enviando response batches: {e}")
            return False
        
    def send_report_batch(self, csv_content: str, query_id: str, is_last_batch: bool = True) -> bool:
        """Envía un batch de reporte al cliente."""
        try:
            last_batch_flag = "1" if is_last_batch else "0"
            # Cambio: usar R{query_id} en lugar de solo R
            message = f"BATCH|R{query_id}|{last_batch_flag}|{csv_content}"
            
            success = self._send_message(message)
            if not success:
                return False
            
            # Esperar ACK del cliente
            ack = self._receive_message()
            if ack != "OK":
                logger.error(f"Cliente no confirmó reporte Q{query_id}: {ack}")
                return False
            
            logger.info(f"Reporte Q{query_id} enviado y confirmado: {len(csv_content)} bytes")
            return True
            
        except Exception as e:
            logger.error(f"Error enviando reporte Q{query_id}: {e}")
            return False
        
    # Método alternativo para enviar por número de líneas CSV en lugar de bytes
    def send_complete_report_by_lines(self, csv_content: str, query_id: str, max_lines_per_batch: int = 1000) -> bool:
        """Envía un reporte completo dividido en batches por número de líneas CSV."""
        try:
            lines = csv_content.splitlines(keepends=True)  # Mantener \n
            
            if len(lines) <= max_lines_per_batch:
                # Enviar en un solo batch
                return self.send_report_batch(csv_content, query_id, True)
            else:
                # Dividir en múltiples batches por líneas
                batches = []
                current_pos = 0
                
                while current_pos < len(lines):
                    # Crear batch con el número de líneas especificado
                    batch_end = min(current_pos + max_lines_per_batch, len(lines))
                    batch_lines = lines[current_pos:batch_end]
                    batch_data = ''.join(batch_lines)
                    batches.append(batch_data)
                    current_pos = batch_end
                
                # Enviar cada batch
                for i, batch_data in enumerate(batches):
                    is_last_batch = (i == len(batches) - 1)
                    success = self.send_report_batch(batch_data, query_id, is_last_batch)
                    if not success:
                        logger.error(f"Error enviando batch {i+1}/{len(batches)} de Q{query_id}")
                        return False
                    
                    lines_in_batch = batch_data.count('\n')
                    logger.debug(f"Batch {i+1}/{len(batches)} de Q{query_id} enviado: {lines_in_batch} líneas, {len(batch_data)} bytes")
                
                logger.info(f"Reporte Q{query_id} completo enviado en {len(batches)} batches")
                return True
                
        except Exception as e:
            logger.error(f"Error enviando reporte completo Q{query_id}: {e}")
            return False

    # ============== RECEPCIÓN DE MENSAJES ==============
    
    def _receive_single_message(self) -> Optional[ProtocolMessage]:
        """Recibe un solo mensaje del socket y lo parsea."""
        try:
            # Recibir header (4 bytes) - longitud del payload
            header_data = self._receive_exact(self.HEADER_SIZE)
            if not header_data:
                return None
            
            payload_length = struct.unpack('>I', header_data)[0]
            
            # Recibir payload completo
            payload_data = self._receive_exact(payload_length)
            if not payload_data:
                return None
            
            # Decodificar payload
            payload = payload_data.decode('utf-8')
            
            # Parsear el mensaje según formato: ACTION|FILE_TYPE|LAST_BATCH|DATA
            return self._parse_message(payload, payload_length)
            
        except Exception as e:
            logger.error(f"Error recibiendo mensaje: {e}")
            return None

    

    def receive_messages(self):
        """Recibe mensajes del cliente usando el parser estructurado."""
        try:
            while True:
                message = self._receive_single_message()
                if message is None:
                    break
                
                # Enviar ACK automáticamente
                self._send_ack()
                
                # Yield del mensaje parseado
                yield message
                
                # Si es EXIT, terminar
                if message.action == "EXIT":
                    break
                    
        except Exception as e:
            logger.error(f"Error recibiendo mensajes: {e}")

    

    def receive_report(self) -> Optional[dict]:
        """Recibe un reporte completo del servidor usando el parser estructurado."""
        try:
            report_parts = []
            query_id = None
            
            while True:
                message = self._receive_single_message()
                if message is None:
                    return None
                
                # Enviar ACK
                self._send_ack()
                
                if message.action == "EXIT":
                    logger.info("EXIT recibido durante recepción de reporte")
                    return None
                
                logger.debug(f"Mensaje recibido: action='{message.action}', file_type='{message.file_type}'")

                # Verificar que es un reporte (file_type empieza con "RQ")
                if message.action == "BATCH" and message.file_type.startswith("RQ"):
                    query_id = message.file_type[2:]  # Extraer número después de "RQ"
                    
                    report_parts.append(message.data)
                    
                    if message.last_batch:
                        complete_report = ''.join(report_parts)
                        logger.info(f"Reporte Q{query_id} completo recibido: {len(complete_report)} bytes")
                        return {
                            'query_id': query_id,
                            'content': complete_report,
                            'total_size': sum(len(part) for part in report_parts)
                        }
                else:
                    logger.warning(f"Mensaje inesperado: {message.action}|{message.file_type}")
                    
        except Exception as e:
            logger.error(f"Error recibiendo reporte: {e}")
            return None

    # Método de conveniencia para enviar reportes completos
    def send_complete_report(self, csv_content: str, query_id: str, max_batch_size: int) -> bool:
        """Envía un reporte completo dividido en batches si es necesario."""
        try:
            if len(csv_content) <= max_batch_size:
                # Enviar en un solo batch
                return self.send_report_batch(csv_content, query_id, True)
            else:
                # Dividir en múltiples batches
                batches = []
                current_pos = 0
                
                while current_pos < len(csv_content):
                    # Crear batch del tamaño especificado
                    batch_end = min(current_pos + max_batch_size, len(csv_content))
                    batch_data = csv_content[current_pos:batch_end]
                    batches.append(batch_data)
                    current_pos = batch_end
                
                # Enviar cada batch
                for i, batch_data in enumerate(batches):
                    is_last_batch = (i == len(batches) - 1)
                    success = self.send_report_batch(batch_data, query_id, is_last_batch)
                    if not success:
                        logger.error(f"Error enviando batch {i+1}/{len(batches)} de Q{query_id}")
                        return False
                    
                    logger.debug(f"Batch {i+1}/{len(batches)} de Q{query_id} enviado: {len(batch_data)} bytes")
                
                logger.info(f"Reporte Q{query_id} completo enviado en {len(batches)} batches")
                return True
                
        except Exception as e:
            logger.error(f"Error enviando reporte completo Q{query_id}: {e}")
            return False


    # ============== MÉTODOS AUXILIARES ==============
    
    def _send_message(self, message: str) -> bool:
        """Envía un mensaje con header de longitud."""
        try:
            data = message.encode('utf-8')
            header = struct.pack('>I', len(data))  # uint32 BigEndian
            
            # Enviar header + payload
            return self._send_all(header + data)
            
        except Exception as e:
            logger.error(f"Error enviando mensaje: {e}")
            return False

    def _receive_message(self) -> Optional[str]:
        """Recibe un mensaje con header de longitud."""
        try:
            # Recibir header (4 bytes)
            header_data = self._receive_exact(self.HEADER_SIZE)
            if not header_data:
                return None
            
            # Extraer longitud del payload
            payload_length = struct.unpack('>I', header_data)[0]
            
            # Recibir payload
            payload_data = self._receive_exact(payload_length)
            if not payload_data:
                return None
            
            return payload_data.decode('utf-8')
            
        except Exception as e:
            logger.error(f"Error recibiendo mensaje: {e}")
            return None

    def _send_ack(self) -> bool:
        """Envía un ACK."""
        return self._send_message("OK")

    def _send_all(self, data: bytes) -> bool:
        """Envía todos los datos manejando short writes."""
        try:
            total_sent = 0
            while total_sent < len(data):
                sent = self.socket_conn.send(data[total_sent:])
                if sent == 0:
                    logger.error("Socket conexión rota")
                    return False
                total_sent += sent
            return True
        except Exception as e:
            logger.error(f"Error enviando datos: {e}")
            return False

    def _receive_exact(self, num_bytes: int) -> Optional[bytes]:
        """Recibe exactamente num_bytes manejando short reads."""
        try:
            data = b''
            while len(data) < num_bytes:
                chunk = self.socket_conn.recv(num_bytes - len(data))
                if not chunk:
                    if len(data) == 0:
                        return None  # Conexión cerrada limpiamente
                    logger.error(f"Conexión cerrada inesperadamente")
                    return None
                data += chunk
            return data
        except Exception as e:
            logger.error(f"Error recibiendo {num_bytes} bytes: {e}")
            return None
        
    def _parse_message(self, payload: str, original_size: int) -> Optional[ProtocolMessage]:
        """Parsea un mensaje según el formato del protocolo."""
        try:
            parts = payload.split('|', 3)  # Máximo 4 partes
            
            if parts[0] == "BATCH" and len(parts) == 4:
                # Formato: BATCH|{file_type}|{last_batch}|{csv_content}
                action = parts[0]
                file_type = parts[1]
                last_batch = parts[2] == "1"
                data = parts[3]
                
                logger.debug(f"Parseando BATCH: file_type='{file_type}', action='{action}'")
                
                return ProtocolMessage(
                    action=action,
                    file_type=file_type,
                    last_batch=last_batch,
                    data=data,
                    size=original_size
                )
                  
            elif parts[0] == "FINISH" and len(parts) == 2:
                # Formato: FINISH|{file_type}
                action = parts[0]
                file_type = parts[1]
                
                return ProtocolMessage(
                    action=action,
                    file_type=file_type,
                    last_batch=True,  # FINISH siempre es "último"
                    data="",
                    size=original_size
                )
                
            elif parts[0] == "EXIT":
                # Formato: EXIT
                return ProtocolMessage(
                    action="EXIT",
                    file_type="",
                    last_batch=True,
                    data="",
                    size=original_size
                )
                
            else:
                logger.error(f"Formato de mensaje desconocido: {payload[:100]}...")
                return None
                
        except Exception as e:
            logger.error(f"Error parseando mensaje: {e}")
            return None

    def close(self):
        """Cierra la conexión."""
        try:
            self.socket_conn.close()
            logger.info("Conexión cerrada")
        except Exception as e:
            logger.error(f"Error cerrando conexión: {e}")