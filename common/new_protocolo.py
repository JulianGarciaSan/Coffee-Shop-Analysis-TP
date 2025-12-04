import struct
import socket
import logging
import threading
import queue
from typing import Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class ProtocolMessage:
    """Representa un mensaje parseado del protocolo."""
    action: str           # BATCH, FINISH, EXIT
    file_type: str        # D, U, S, q1, q2, etc.
    last_batch: bool      # True/False
    data: str             # Contenido CSV
    size: int             # Tamaño del payload original

class ProtocolNew:
    # Tipos de mensaje
    MSG_TYPE_BATCH = 0x01    
    MSG_TYPE_ACK = 0x02     
    MSG_TYPE_PUSH = 0x03     
    MSG_TYPE_EXIT = 0x04     
    
    HEADER_SIZE = 9  


    def __init__(self, socket_conn: socket.socket):
        self.socket_conn = socket_conn
        self.next_msg_id = 1
        self.pending_acks = {} 
        self.report_queue = queue.Queue()
        self._running = True
        
        self.receiver_thread = threading.Thread(target=self._receiver_loop, daemon=True)
        self.receiver_thread.start()
        
        self.popped_report_lines = 0


    
    def _receiver_loop(self):
        """Lee TODO del socket y distribuye según tipo de mensaje."""
        try:
            while self._running:
                header = self._receive_exact(self.HEADER_SIZE)
                if not header:
                    break
                
                length = struct.unpack('>I', header[:4])[0]
                msg_type = header[4]
                msg_id = struct.unpack('>I', header[5:9])[0]
                
                payload = self._receive_exact(length) if length > 0 else b''
                
                if msg_type == self.MSG_TYPE_ACK:
                    event = self.pending_acks.get(msg_id)
                    if event:
                        event.set()
                        
                elif msg_type == self.MSG_TYPE_PUSH:
                    if payload:
                        decoded_payload = payload.decode('utf-8')
                    # EXIT_REPORTS es crítico → enviar ACK
                        if decoded_payload == "EXIT_REPORTS":
                            self._send_ack(msg_id)  # ← ACK obligatorio
                            self.report_queue.put("EXIT")
                            logger.info("EXIT_REPORTS recibido y confirmado con ACK")
                        else:
                            # Datos normales → NO ACK (fire-and-forget)
                            self.report_queue.put(decoded_payload)
                            # NO enviar ACK para throughput
                        
                elif msg_type == self.MSG_TYPE_BATCH:
                    if payload:
                        message = self._parse_message(payload.decode('utf-8'), length)
                        if message:
                            self._send_ack(msg_id)
                            self.report_queue.put(("COMMAND", message))
                elif msg_type == self.MSG_TYPE_EXIT:
                    self.report_queue.put("EXIT")
                    break
                    
        except Exception as e:
            logger.error(f"Error en receiver loop: {e}")
        finally:
            logger.info("Receiver loop terminado")
    
    def send_batch_message(self, batch_result, file_type: str) -> bool:
        """Envía un batch de datos al servidor."""
        try:
            if hasattr(batch_result, 'data') and batch_result.data:
                csv_content = batch_result.data  
            elif hasattr(batch_result, 'items') and batch_result.items:
                csv_content = '\n'.join(batch_result.items)
            else:
                logger.error(f"BatchResult no tiene data ni items válidos")
                return False
            
            is_last_batch = batch_result.is_last_batch if hasattr(batch_result, 'is_last_batch') else False
            
            last_batch_flag = "1" if is_last_batch else "0"
            message = f"BATCH|{file_type}|{last_batch_flag}|{csv_content}"
            
            return self._send_with_ack(self.MSG_TYPE_BATCH, message)
            
        except Exception as e:
            logger.error(f"Error enviando batch: {e}")
            return False

    def send_finish_message(self, file_type: str) -> bool:
        """Envía mensaje FINISH."""
        try:
            message = f"FINISH|{file_type}"
            return self._send_with_ack(self.MSG_TYPE_BATCH, message)
        except Exception as e:
            logger.error(f"Error enviando FINISH: {e}")
            return False

    def send_exit_message(self) -> bool:
        """Envía mensaje EXIT."""
        try:
            message = "EXIT"
            return self._send_with_ack(self.MSG_TYPE_BATCH, message)
        except Exception as e:
            logger.error(f"Error enviando EXIT: {e}")
            return False

    def send_report_data_to_client(self, data, query_name):
        """Envía datos de reporte SIN esperar ACK (fire-and-forget)."""
        try:
            message = f"L|{query_name}|0|{data}"
            return self._send_push_no_ack(message)  # ← Nuevo método
        except Exception as e:
            logger.error(f"Error enviando datos de reporte: {e}")
            return False
        
    def _send_push_no_ack(self, message: str) -> bool:
        """Envía PUSH sin esperar ACK (fire-and-forget para throughput)."""
        msg_id = self.next_msg_id
        self.next_msg_id += 1
        return self._send_with_header(self.MSG_TYPE_PUSH, msg_id, message)
        
    def send_reports_exit_message(self):
        """Envía EXIT_REPORTS como PUSHp (con ACK)."""
        try:
            message = "EXIT_REPORTS"
            return self._send_push(message)
        except Exception as e:
            logger.error(f"Error enviando EXIT_REPORTS: {e}")
            return False

    def _send_with_ack(self, msg_type: int, message: str) -> bool:
        """Envía mensaje y espera ACK."""
        msg_id = self.next_msg_id
        self.next_msg_id += 1
        
        ack_event = threading.Event()
        self.pending_acks[msg_id] = ack_event
        
        try:
            success = self._send_with_header(msg_type, msg_id, message)
            if not success:
                return False
            
            if ack_event.wait(timeout=5.0):
                return True
            else:
                logger.error(f"Timeout esperando ACK para msg_id {msg_id}")
                return False
        finally:
            self.pending_acks.pop(msg_id, None)

    def _send_push(self, message: str) -> bool:
        msg_id = self.next_msg_id
        self.next_msg_id += 1
        
        # Preparar para esperar ACK
        ack_event = threading.Event()
        self.pending_acks[msg_id] = ack_event
        
        try:
            # Enviar mensaje
            success = self._send_with_header(self.MSG_TYPE_PUSH, msg_id, message)
            if not success:
                return False
            
            # Esperar ACK
            if ack_event.wait(timeout=10.0):
                return True
            else:
                logger.error(f"Timeout esperando ACK para PUSH msg_id {msg_id}")
                return False
        finally:
            # Limpiar
            self.pending_acks.pop(msg_id, None)

    def _send_ack(self, msg_id: int) -> bool:
        """Envía ACK para un mensaje específico."""
        return self._send_with_header(self.MSG_TYPE_ACK, msg_id, "")

    def _send_with_header(self, msg_type: int, msg_id: int, message: str) -> bool:
        """Envía mensaje con header completo."""
        try:
            data = message.encode('utf-8')
            
            header = struct.pack('>I B I', len(data), msg_type, msg_id)
            
            return self._send_all(header + data)
            
        except Exception as e:
            logger.error(f"Error enviando mensaje con header: {e}")
            return False

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

    def receive_messages(self):
        """Recibe mensajes procesados de la cola."""
        try:
            while True:
                item = self.report_queue.get()
                
                if item == "EXIT":
                    logger.info("EXIT recibido")
                    break
                
                if isinstance(item, tuple) and item[0] == "COMMAND":
                    yield item[1]
                elif isinstance(item, str) and item.startswith("L|"):
                    parts = item.split('|', 3)
                    if len(parts) == 4:
                        yield ProtocolMessage(
                            action=parts[0],
                            file_type=parts[1],
                            last_batch=parts[2] == "1",
                            data=parts[3],
                            size=len(item)
                        )
                        
        except Exception as e:
            logger.error(f"Error recibiendo mensajes: {e}")

    def receive_reports(self):
        """Recibe solo reportes de la cola."""
        try:
            while True:
                item = self.report_queue.get()
                
                if item == "EXIT":
                    logger.info("EXIT recibido en reportes")
                    yield ProtocolMessage(
                        action="EXIT",
                        file_type="",
                        last_batch=True,
                        data="",
                        size=0
                    )                    
                    break   
                
                if isinstance(item, str) and item.startswith("L|"):
                    parts = item.split('|', 3)
                    yield ProtocolMessage(
                        action=parts[0],
                        file_type=parts[1],
                        last_batch=parts[2] == "1",
                        data=parts[3],
                        size=len(item)
                    )
                        
        except Exception as e:
            logger.error(f"Error recibiendo reportes: {e}")

    def _receive_exact(self, num_bytes: int) -> Optional[bytes]:
        """Recibe exactamente num_bytes manejando short reads."""
        try:
            data = b''
            while len(data) < num_bytes:
                chunk = self.socket_conn.recv(num_bytes - len(data))
                if not chunk:
                    if len(data) == 0:
                        return None 
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
            parts = payload.split('|', 3)  
            
            if parts[0] == "BATCH" and len(parts) == 4:
                return ProtocolMessage(
                    action=parts[0],
                    file_type=parts[1],
                    last_batch=parts[2] == "1",
                    data=parts[3],
                    size=original_size
                )
            elif parts[0] == "FINISH" and len(parts) == 2:
                return ProtocolMessage(
                    action=parts[0],
                    file_type=parts[1],
                    last_batch=True,
                    data="",
                    size=original_size
                )
            elif parts[0] == "EXIT":
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
            self._running = False
            self.socket_conn.close()
            if self.receiver_thread.is_alive():
                self.receiver_thread.join(timeout=2.0)
            logger.info("Conexión cerrada")
        except Exception as e:
            logger.error(f"Error cerrando conexión: {e}")
            
    # def _receive_single_message(self) -> Optional[ProtocolMessage]:
    #     """Recibe un solo mensaje del socket y lo parsea."""
    #     try:
    #         # Recibir header (4 bytes) - longitud del payload
    #         header_data = self._receive_exact(self.HEADER_SIZE)
    #         if not header_data:
    #             return None
            
    #         payload_length = struct.unpack('>I', header_data)[0]
            
    #         # Recibir payload completo
    #         payload_data = self._receive_exact(payload_length)
    #         if not payload_data:
    #             return None
            
    #         # Decodificar payload
    #         payload = payload_data.decode('utf-8')
            
    #         # Parsear el mensaje según formato: ACTION|FILE_TYPE|LAST_BATCH|DATA
    #         return (payload, payload_length)
    #         #return self._parse_message(payload, payload_length)
            
    #     except Exception as e:
    #         logger.error(f"Error recibiendo mensaje: {e}")
    #         return None
            
    # def send_exit_message(self) -> bool:
    #     """Envía mensaje EXIT al servidor."""
    #     try:
    #         success = self._send_message("EXIT")
    #         if not success:
    #             return False
            
    #         return self._wait_for_ack()
    #         # ack = self._receive_message()
    #         # if ack != "OK":
    #         #     logger.error(f"ACK inválido para EXIT: {ack}")
    #         #     return False
            
    #         # logger.info("Mensaje EXIT enviado y confirmado")
    #         # return True
            
    #     except Exception as e:
    #         logger.error(f"Error enviando EXIT: {e}")
    #         return False
            
    # def send_message(self, message: str) -> bool:
    #     try:
    #         return self._send_message(message)
    #     except Exception as e:
    #         logger.error(f"Error enviando mensaje: {e}")
    #         return False       
            
    # def send_response_batches(self, action: str, file_type: str, csv_content: str) -> bool:
    #     """Envía datos en múltiples batches como respuesta (método de compatibilidad)."""
    #     try:
    #         # Extraer query_id del action
    #         # Ej: "RPRT_Q1" -> query_id = "1"
    #         if action.startswith("RPRT_Q"):
    #             query_id = action[5:]  # Extraer número después de "RPRT_Q"
    #         elif action.startswith("RPRT_"):
    #             query_id = action[5:]  # Extraer parte después de "RPRT_"
    #         else:
    #             query_id = "1"  # Default
            
    #         # Usar el método que ya existe
    #         return self.send_complete_report(csv_content, query_id, 50000)
            
    #     except Exception as e:
    #         logger.error(f"Error enviando response batches: {e}")
    #         return False
        
    # def send_report_batch(self, csv_content: str, query_id: str, is_last_batch: bool = True) -> bool:
    #     """Envía un batch de reporte al cliente."""
    #     try:
    #         last_batch_flag = "1" if is_last_batch else "0"
    #         # Cambio: usar R{query_id} en lugar de solo R
    #         message = f"BATCH|R{query_id}|{last_batch_flag}|{csv_content}"
            
    #         success = self._send_message(message)
    #         if not success:
    #             return False
            
    #         # Esperar ACK del cliente
    #         ack = self._receive_message()
    #         if ack != "OK":
    #             logger.error(f"Cliente no confirmó reporte Q{query_id}: {ack}")
    #             return False
            
    #         logger.info(f"Reporte Q{query_id} enviado y confirmado: {len(csv_content)} bytes")
    #         return True
            
    #     except Exception as e:
    #         logger.error(f"Error enviando reporte Q{query_id}: {e}")
    #         return False
        
    # # Método alternativo para enviar por número de líneas CSV en lugar de bytes
    # def send_complete_report_by_lines(self, csv_content: str, query_id: str, max_lines_per_batch: int = 1000) -> bool:
    #     """Envía un reporte completo dividido en batches por número de líneas CSV."""
    #     try:
    #         lines = csv_content.splitlines(keepends=True)  # Mantener \n
            
    #         if len(lines) <= max_lines_per_batch:
    #             # Enviar en un solo batch
    #             return self.send_report_batch(csv_content, query_id, True)
    #         else:
    #             # Dividir en múltiples batches por líneas
    #             batches = []
    #             current_pos = 0
                
    #             while current_pos < len(lines):
    #                 # Crear batch con el número de líneas especificado
    #                 batch_end = min(current_pos + max_lines_per_batch, len(lines))
    #                 batch_lines = lines[current_pos:batch_end]
    #                 batch_data = ''.join(batch_lines)
    #                 batches.append(batch_data)
    #                 current_pos = batch_end
                
    #             # Enviar cada batch
    #             for i, batch_data in enumerate(batches):
    #                 is_last_batch = (i == len(batches) - 1)
    #                 success = self.send_report_batch(batch_data, query_id, is_last_batch)
    #                 if not success:
    #                     logger.error(f"Error enviando batch {i+1}/{len(batches)} de Q{query_id}")
    #                     return False
                    
    #                 lines_in_batch = batch_data.count('\n')
    #                 logger.debug(f"Batch {i+1}/{len(batches)} de Q{query_id} enviado: {lines_in_batch} líneas, {len(batch_data)} bytes")
                
    #             logger.info(f"Reporte Q{query_id} completo enviado en {len(batches)} batches")
    #             return True
                
    #     except Exception as e:
    #         logger.error(f"Error enviando reporte completo Q{query_id}: {e}")
            # return False
            
            
        

    # def receive_report(self) -> Optional[dict]:
    #     """Recibe un reporte completo del servidor usando el parser estructurado."""
    #     try:
    #         report_parts = []
    #         query_id = None
            
    #         while True:
    #             message = self._receive_single_message()
    #             if message is None:
    #                 return None
                
    #             # Enviar ACK
    #             self._send_ack()
                
    #             if message.action == "EXIT":
    #                 logger.info("EXIT recibido durante recepción de reporte")
    #                 return None
                
    #             logger.debug(f"Mensaje recibido: action='{message.action}', file_type='{message.file_type}'")

    #             # Verificar que es un reporte (file_type empieza con "RQ")
    #             if message.action == "BATCH" and message.file_type.startswith("RQ"):
    #                 query_id = message.file_type[2:]  # Extraer número después de "RQ"
                    
    #                 report_parts.append(message.data)
                    
    #                 if message.last_batch:
    #                     complete_report = ''.join(report_parts)
    #                     logger.info(f"Reporte Q{query_id} completo recibido: {len(complete_report)} bytes")
    #                     return {
    #                         'query_id': query_id,
    #                         'content': complete_report,
    #                         'total_size': sum(len(part) for part in report_parts)
    #                     }
    #             else:
    #                 logger.warning(f"Mensaje inesperado: {message.action}|{message.file_type}")
                    
    #     except Exception as e:
    #         logger.error(f"Error recibiendo reporte: {e}")
    #         return None

    # # Método de conveniencia para enviar reportes completos
    # def send_complete_report(self, csv_content: str, query_id: str, max_batch_size: int) -> bool:
    #     """Envía un reporte completo dividido en batches si es necesario."""
    #     try:
    #         if len(csv_content) <= max_batch_size:
    #             # Enviar en un solo batch
    #             return self.send_report_batch(csv_content, query_id, True)
    #         else:
    #             # Dividir en múltiples batches
    #             batches = []
    #             current_pos = 0
                
    #             while current_pos < len(csv_content):
    #                 # Crear batch del tamaño especificado
    #                 batch_end = min(current_pos + max_batch_size, len(csv_content))
    #                 batch_data = csv_content[current_pos:batch_end]
    #                 batches.append(batch_data)
    #                 current_pos = batch_end
                
    #             # Enviar cada batch
    #             for i, batch_data in enumerate(batches):
    #                 is_last_batch = (i == len(batches) - 1)
    #                 success = self.send_report_batch(batch_data, query_id, is_last_batch)
    #                 if not success:
    #                     logger.error(f"Error enviando batch {i+1}/{len(batches)} de Q{query_id}")
    #                     return False
                    
    #                 logger.debug(f"Batch {i+1}/{len(batches)} de Q{query_id} enviado: {len(batch_data)} bytes")
                
    #             logger.info(f"Reporte Q{query_id} completo enviado en {len(batches)} batches")
    #             return True
                
    #     except Exception as e:
    #         logger.error(f"Error enviando reporte completo Q{query_id}: {e}")
    #         return False