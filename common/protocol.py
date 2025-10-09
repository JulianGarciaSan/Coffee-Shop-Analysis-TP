import socket
import struct
import logging
from dataclasses import dataclass
from typing import Generator, Optional, List
from common.processor import BatchResult

logger = logging.getLogger(__name__)

MAX_MESSAGE_SIZE = 65535
ACTION_EXIT = "EXIT"
ACTION_SEND = "SEND"

@dataclass
class ProtocolMessage:
    action: str
    file_type: str
    size: int
    last_batch: bool
    data: str

@dataclass
class AckMessage:
    batch_number: int
    status: int

class Protocol:
    def __init__(self, socket_conn: socket.socket):
        self.socket_conn = socket_conn
        self.batch_counter = 0

    def send_batch_message(self, action: str, file_type: str, data: str, is_last_batch: bool = False) -> bool:
        """Envía un mensaje batch al servidor."""
        try:
            message_size = len(data) if data else 0
            
            if message_size > MAX_MESSAGE_SIZE:
                logger.error(f"Mensaje demasiado largo: {message_size} bytes")
                return False
            
            # Crear el mensaje
            message = bytearray()
            message.extend(action.ljust(4, '\x00').encode('utf-8'))
            message.extend(file_type.encode('utf-8'))
            message.extend(struct.pack('>I', message_size))
            message.extend(struct.pack('B', 1 if is_last_batch else 0))
            
            if data:
                message.extend(data.encode('utf-8'))
            
            # Enviar el mensaje
            success = self._send_all(bytes(message))
            if not success:
                return False
            
            # Esperar ACK del servidor
            ack_response = self._receive_ack()
            if ack_response is None:
                logger.error("No se recibió ACK del servidor")
                return False
            
            if ack_response.status != 0:
                logger.error(f"Servidor respondió con error: status={ack_response.status}")
                return False
            
            logger.info(f"Batch enviado y confirmado: {len(data)} items, action={action}")
            return True
            
        except Exception as e:
            logger.error(f"Error enviando batch: {e}")
            return False

    def send_finish_message(self, file_type: str) -> bool:
        """Envía mensaje FINISH al servidor."""
        try:
            message = bytearray()
            message.extend("FINI".encode('utf-8'))
            message.extend(file_type.encode('utf-8'))
            message.extend(struct.pack('>I', 0))  # Size = 0
            message.extend(struct.pack('B', 1))   # Last batch = True
            
            success = self._send_all(bytes(message))
            if not success:
                return False
            
            # Esperar ACK del servidor
            ack_response = self._receive_ack()
            if ack_response is None:
                logger.error("No se recibió ACK del servidor para FINISH")
                return False
            
            if ack_response.status != 0:
                logger.error(f"Servidor respondió con error para FINISH: status={ack_response.status}")
                return False
            
            logger.info(f"Mensaje FINISH enviado y confirmado para file_type: {file_type}")
            return True
            
        except Exception as e:
            logger.error(f"Error enviando FINISH: {e}")
            return False

    def send_exit_message(self) -> bool:
        """Envía mensaje EXIT al servidor."""
        try:
            message = bytearray()
            message.extend("EXIT".encode('utf-8'))
            message.extend("E".encode('utf-8'))
            message.extend(struct.pack('>I', 0))
            message.extend(struct.pack('B', 1))
            
            success = self._send_all(bytes(message))
            if not success:
                return False
            
            # Esperar ACK del servidor
            ack_response = self._receive_ack()
            if ack_response is None:
                logger.error("No se recibió ACK del servidor para EXIT")
                return False
            
            if ack_response.status != 0:
                logger.error(f"Servidor respondió con error para EXIT: status={ack_response.status}")
                return False
            
            logger.info("Mensaje EXIT enviado y confirmado")
            return True
            
        except Exception as e:
            logger.error(f"Error enviando EXIT: {e}")
            return False

    def receive_messages(self) -> Generator[ProtocolMessage, None, None]:
        """Recibe mensajes del cliente."""
        try:
            while True:
                message = self._receive_single_message()
                if message is None:
                    break
                
                yield message
                
                # Enviar ACK después de procesar cada mensaje
                self._send_ack(self.batch_counter, 0)  # 0 = Success
                self.batch_counter += 1
                    
        except Exception as e:
            logger.error(f"Error recibiendo mensajes: {e}")
            try:
                self._send_ack(self.batch_counter, 2)  # 2 = Error
            except:
                pass

    def send_response_message(self, action: str, file_type: str, data: str, is_last_batch: bool = True) -> bool:
        """Envía un mensaje de respuesta del servidor al cliente (como un reporte)."""
        try:
            message_size = len(data) if data else 0
            
            if message_size > MAX_MESSAGE_SIZE:
                logger.error(f"Mensaje de respuesta demasiado largo: {message_size} bytes")
                return False
            
            # Crear el mensaje: [ACTION(4)] + [FILE-TYPE(1)] + [SIZE(4)] + [LAST-BATCH(1)] + [DATA(N)]
            message = bytearray()
            
            # ACTION (4 bytes, padded with nulls)
            message.extend(action.ljust(4, '\x00').encode('utf-8'))
            
            # FILE-TYPE (1 byte)
            message.extend(file_type.encode('utf-8'))
            
            # SIZE (4 bytes, big endian)
            message.extend(struct.pack('>I', message_size))
            
            # LAST-BATCH (1 byte)
            message.extend(struct.pack('B', 1 if is_last_batch else 0))
            
            # DATA (N bytes)
            if data:
                message.extend(data.encode('utf-8'))
            
            # Enviar el mensaje
            success = self._send_all(bytes(message))
            
            if not success:
                return False
            
            # Esperar ACK del cliente
            ack_response = self._receive_ack()
            if ack_response is None:
                logger.error("No se recibió ACK del cliente")
                return False
            
            if ack_response.status != 0:  # 0 = Success
                logger.error(f"Cliente respondió con error: status={ack_response.status}")
                return False
            
            logger.info(f"Mensaje de respuesta enviado y confirmado: action={action}, file_type={file_type}, size={message_size}")
            return True
            
        except Exception as e:
            logger.error(f"Error enviando mensaje de respuesta: {e}")
            return False

    def send_response_batches(self, action: str, file_type: str, data: str) -> bool:
        """Envía datos en múltiples batches como respuesta."""
        try:
            if not data:
                # Enviar mensaje vacío
                return self.send_response_message(action, file_type, "", True)
            
            chunks = [data[i:i + MAX_MESSAGE_SIZE] for i in range(0, len(data), MAX_MESSAGE_SIZE)]
            
            for i, chunk in enumerate(chunks):
                is_last = (i == len(chunks) - 1)
                
                success = self.send_response_message(action, file_type, chunk, is_last)
                if not success:
                    logger.error(f"Error enviando batch {i+1}/{len(chunks)}")
                    return False
            
            logger.info(f"Todos los batches enviados correctamente: {len(chunks)} batches")
            return True
            
        except Exception as e:
            logger.error(f"Error enviando response batches: {e}")
            return False

    def receive_report(self) -> Optional[str]:
        """
        Recibe un reporte completo del servidor.
        Retorna el contenido completo del reporte o None si hay error/conexión cerrada.
        """
        try:
            report_data = []
            
            while True:
                message = self._receive_single_message()
                
                if message is None:
                    logger.info("Conexión cerrada durante recepción de reporte")
                    return None
                
                # Enviar ACK por cada mensaje recibido
                self._send_ack(self.batch_counter, 0)  # 0 = Success
                self.batch_counter += 1
                
                if message.action == "EXIT":
                    logger.info("Mensaje EXIT recibido durante recepción de reporte")
                    return None
                    
                if message.action == "EOF":
                    logger.info("EOF recibido - no hay más reportes")
                    return None
                
                # Si es un mensaje de reporte, acumular datos
                if message.action.startswith("RPRT_"):
                    report_data.append(message.data)
                    
                    # Si es el último batch, retornar reporte completo
                    if message.last_batch:
                        complete_report = ''.join(report_data)
                        logger.info(f"Reporte completo recibido: {len(complete_report)} bytes")
                        return complete_report
                else:
                    logger.warning(f"Mensaje inesperado durante recepción de reporte: {message.action}")
                    
        except Exception as e:
            logger.error(f"Error recibiendo reporte: {e}")
            return None

    def _receive_single_message(self) -> Optional[ProtocolMessage]:
        """Recibe un solo mensaje del socket."""
        try:
            # Recibir ACTION (4 bytes)
            action_data = self._receive_exact(4)
            if not action_data:
                return None
            action = action_data.decode('utf-8').rstrip('\x00')

            # Recibir FILE-TYPE (1 byte)
            file_type_data = self._receive_exact(1)
            if not file_type_data:
                return None
            file_type = file_type_data.decode('utf-8')

            # Recibir SIZE (4 bytes)
            size_data = self._receive_exact(4)
            if not size_data:
                return None
            size = struct.unpack('>I', size_data)[0]

            # Recibir LAST-BATCH (1 byte)
            last_batch_data = self._receive_exact(1)
            if not last_batch_data:
                return None
            last_batch = struct.unpack('B', last_batch_data)[0] == 1

            # Recibir DATA (size bytes)
            data = ""
            if size > 0:
                data_bytes = self._receive_exact(size)
                if not data_bytes:
                    return None
                data = data_bytes.decode('utf-8')

            return ProtocolMessage(action, file_type, size, last_batch, data)

        except Exception as e:
            logger.error(f"Error recibiendo mensaje: {e}")
            return None

    def _send_ack(self, batch_number: int, status: int) -> bool:
        """Envía un ACK al otro extremo."""
        try:
            # Formato ACK: [BATCH_NUMBER(4)] + [STATUS(1)]
            ack_message = bytearray()
            ack_message.extend(struct.pack('>I', batch_number))  # 4 bytes big endian
            ack_message.extend(struct.pack('B', status))         # 1 byte
            
            success = self._send_all(bytes(ack_message))
            if success:
                logger.debug(f"ACK enviado: batch={batch_number}, status={status}")
            return success
            
        except Exception as e:
            logger.error(f"Error enviando ACK: {e}")
            return False

    def _receive_ack(self) -> Optional[AckMessage]:
        """Recibe un ACK del otro extremo."""
        try:
            # Recibir BATCH_NUMBER (4 bytes)
            batch_data = self._receive_exact(4)
            if not batch_data:
                return None
            batch_number = struct.unpack('>I', batch_data)[0]
            
            # Recibir STATUS (1 byte)
            status_data = self._receive_exact(1)
            if not status_data:
                return None
            status = struct.unpack('B', status_data)[0]
            
            logger.debug(f"ACK recibido: batch={batch_number}, status={status}")
            return AckMessage(batch_number, status)
            
        except Exception as e:
            logger.error(f"Error recibiendo ACK: {e}")
            return None

    def _send_all(self, data: bytes) -> bool:
        """Envía todos los datos por el socket."""
        try:
            total_sent = 0
            while total_sent < len(data):
                sent = self.socket_conn.send(data[total_sent:])
                if sent == 0:
                    logger.error("Socket conexión rota al enviar")
                    return False
                total_sent += sent
            return True
        except Exception as e:
            logger.error(f"Error enviando datos: {e}")
            return False

    def _receive_exact(self, num_bytes: int) -> Optional[bytes]:
        """Recibe exactamente num_bytes del socket."""
        try:
            data = b''
            while len(data) < num_bytes:
                chunk = self.socket_conn.recv(num_bytes - len(data))
                if not chunk:
                    if len(data) == 0:
                        return None  # Conexión cerrada limpiamente
                    logger.error(f"Conexión cerrada inesperadamente. Esperaba {num_bytes} bytes, recibió {len(data)}")
                    return None
                data += chunk
            return data
        except Exception as e:
            logger.error(f"Error recibiendo {num_bytes} bytes: {e}")
            return None

    def close(self):
        """Cierra la conexión del socket."""
        try:
            self.socket_conn.close()
            logger.info("Conexión cerrada")
        except Exception as e:
            logger.error(f"Error cerrando conexión: {e}")