# hour_node_configurator.py

import logging
import os
import threading
from typing import Optional, Dict, Any
from rabbitmq.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from dtos.dto import TransactionBatchDTO, BatchType, CoordinationMessageDTO
from .base_configurator import NodeConfigurator
from coordinator.coordinator import PeerCoordinator 
logger = logging.getLogger(__name__)


class HourNodeConfigurator(NodeConfigurator):
    def __init__(self, rabbitmq_host: str):
        super().__init__(rabbitmq_host)
        self.node_id = os.getenv('NODE_ID', f'hour_node_{os.getpid()}')
        self.total_nodes = int(os.getenv('TOTAL_HOUR_FILTERS', '1'))
        all_node_ids_str = os.getenv('ALL_NODE_IDS', self.node_id)
        all_node_ids = [nid.strip() for nid in all_node_ids_str.split(',')]
        
        self.coordinator = PeerCoordinator(
            node_id=self.node_id,
            rabbitmq_host=rabbitmq_host,
            total_nodes=self.total_nodes,
            all_node_ids=all_node_ids,
            exchange_name='coordination_hour_exchange' 
        )
        
        self.coordination_queue = MessageMiddlewareQueue(
            host=rabbitmq_host,
            queue_name=f'coordination_{self.node_id}',
            exchange_name='coordination_hour_exchange',
            routing_keys=['coord.#']
        )
        
        self.coordination_thread: Optional[threading.Thread] = None
        self.coordination_running = False
        
        self.output_middlewares: Optional[Dict[str, Any]] = None
        
        self._start_coordination_thread()
        
        logger.info(f"HourNodeConfigurator inicializado con coordinación multi-cliente")
        logger.info(f"  Node ID: {self.node_id}")
        logger.info(f"  Total nodos: {self.total_nodes}")
    
    def _start_coordination_thread(self):
        self.coordination_running = True
        self.coordination_thread = threading.Thread(
            target=self._run_coordination_consumer,
            daemon=True,
            name=f"Coordination-{self.node_id}"
        )
        self.coordination_thread.start()
        logger.info(f"Thread de coordinación iniciado para {self.node_id}")
    
    def _run_coordination_consumer(self):
        logger.info(f"Escuchando mensajes de coordinación en: coordination_{self.node_id}")
        try:
            self.coordination_queue.start_consuming(self._on_coordination_message)
        except Exception as e:
            if self.coordination_running:
                logger.error(f"Error en thread de coordinación: {e}")
    
    def _on_coordination_message(self, ch, method, properties, body):
        try:
            msg = CoordinationMessageDTO.from_bytes_fast(body)
            
            if msg.msg_type == CoordinationMessageDTO.EOF_FANOUT:
                self.coordinator.handle_eof_fanout_received(
                    msg.client_id,
                    msg.node_id,
                    msg.batch_type_str
                )
            
            elif msg.msg_type == CoordinationMessageDTO.ACK:
                self.coordinator.handle_ack_received(
                    msg.client_id,
                    msg.node_id,
                    msg.batch_type_str
                )
            
            else:
                logger.warning(f"Tipo de mensaje desconocido: {msg.msg_type}")
        
        except Exception as e:
            logger.error(f"Error procesando mensaje de coordinación: {e}")
    

    def create_input_middleware(self, input_queue: str, node_id: str):
        logger.info(f"HourNode: Usando working queue compartida '{input_queue}'")
        
        return MessageMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=input_queue
        )
        
    def create_output_middlewares(self, output_q1: Optional[str], output_q3: Optional[str],
                                  output_q4: Optional[str] = None, output_q2: Optional[str] = None) -> Dict[str, Any]:
        middlewares = {}
        logger.info(f"Configurando middlewares de salida para HourNodeConfigurator {output_q1}, {output_q3}")

        if output_q1:
            middlewares['q1'] = MessageMiddlewareQueue(
                host=self.rabbitmq_host,
                queue_name=output_q1
            )
            logger.info(f"  Output Q1 Queue: {output_q1}")
        
        if output_q3:
            middlewares['q3'] = MessageMiddlewareExchange(
                host=self.rabbitmq_host,
                exchange_name=output_q3,
                route_keys=['semester.1', 'semester.2', 'eof.all']
            )
            logger.info(f"  Output Q3 Exchange: {output_q3}")
        
        self.output_middlewares = middlewares
        
        return middlewares
    
    def process_filtered_data(self, filtered_csv: str) -> str:
        return filtered_csv

    def process_message(self, body: bytes, routing_key: str = None, client_id: Optional[int] = None) -> tuple:
        decoded_data = body.decode('utf-8').strip()
        
        client_id_str = str(client_id) if client_id is not None else "default"
        
        # Manejar EOF
        if decoded_data.startswith("EOF:"):
            logger.info(f"EOF recibido para cliente {client_id_str}")
            
            # Tomar liderazgo
            self.coordinator.take_leadership(
                client_id_str, 
                'transactions',
                self._on_all_acks_received
            )
            
            # Retornar should_stop=False para que main.py NO lo trate como EOF
            dto = TransactionBatchDTO(decoded_data, BatchType.EOF)
            return (False, 'transactions', dto, False)
        
        # Verificar si debo procesar este mensaje (coordinación)
        if not self.coordinator.should_process_message(client_id_str):
            logger.info(f"Cliente {client_id_str} ya finalizó, ignorando mensaje")
            dto = TransactionBatchDTO(decoded_data, BatchType.RAW_CSV)
            return (True, 'transactions', dto, False)
        
        # Procesar mensaje normalmente
        dto = TransactionBatchDTO(decoded_data, BatchType.RAW_CSV)
        return (False, 'transactions', dto, False)

    def send_data(self, data: str, middlewares: Dict[str, Any], batch_type: str = "transactions", client_id: Optional[int] = None):
        headers = self.create_headers(client_id)
        
        # Verificar si debo enviar ACK después de enviar datos
        if client_id:
            client_id_str = str(client_id)
            if self.coordinator.should_send_ack_after_processing(client_id_str):
                self.coordinator.send_ack(client_id_str, 'transactions')
        
        if 'q1' in middlewares:
            filtered_dto = TransactionBatchDTO(data, batch_type=BatchType.RAW_CSV)
            middlewares['q1'].send(filtered_dto.to_bytes_fast(), headers=headers)
        
        if 'q3' in middlewares:
            self._send_to_exchange_by_semester(data, middlewares['q3'], client_id)

    def _on_all_acks_received(self, client_id: str, batch_type: str):
        logger.info(f"Todos los ACKs recibidos para cliente {client_id}, propagando EOF downstream")
        
        if self.output_middlewares is None:
            logger.error("output_middlewares no está configurado")
            return
        
        client_id_int = int(client_id) if client_id.isdigit() else None
        self.send_eof(self.output_middlewares, "transactions", client_id=client_id_int)

    def send_eof(self, middlewares: Dict[str, Any], batch_type: str = "transactions", client_id: Optional[int] = None):
        headers = self.create_headers(client_id)
        eof_dto = TransactionBatchDTO("EOF:1", BatchType.EOF)
        
        if 'q1' in middlewares:
            middlewares['q1'].send(eof_dto.to_bytes_fast(), headers=headers)
            logger.info(f"EOF enviado a Q1 (amount queue) para cliente {client_id}")
        
        if 'q3' in middlewares:
            middlewares['q3'].send(
                eof_dto.to_bytes_fast(),
                routing_key='eof.all',
                headers=headers
            )
            logger.info(f"EOF enviado a Q3 exchange para cliente {client_id}")
            
    def _send_to_exchange_by_semester(self, csv_data: str, exchange_middleware, client_id: Optional[int] = None):
        headers = self.create_headers(client_id)
        semester_1_lines = []
        semester_2_lines = []
        
        for line in csv_data.split('\n'):
            if not line.strip():
                continue
            
            month = self._get_month_from_csv_line(line)
            if month:
                if month <= 6:
                    semester_1_lines.append(line)
                else:
                    semester_2_lines.append(line)
        
        if semester_1_lines:
            csv_s1 = '\n'.join(semester_1_lines)
            dto_s1 = TransactionBatchDTO(csv_s1, batch_type=BatchType.RAW_CSV)
            exchange_middleware.send(dto_s1.to_bytes_fast(), routing_key='semester.1', headers=headers)
        
        if semester_2_lines:
            csv_s2 = '\n'.join(semester_2_lines)
            dto_s2 = TransactionBatchDTO(csv_s2, batch_type=BatchType.RAW_CSV)
            exchange_middleware.send(dto_s2.to_bytes_fast(), routing_key='semester.2', headers=headers)
    
    @staticmethod
    def _get_month_from_csv_line(line):
        """Extract month from CSV line"""
        fields = line.split(',')
        if len(fields) >= 9:
            date_str = fields[8]
            return int(date_str[5:7])
        return None
    
    def handle_eof(self, counter: int, total_filters: int, eof_type: str, 
                   middlewares: Dict[str, Any], input_middleware: Any, client_id: Optional[int] = None) -> bool:
        """
        Ya NO se usa - el coordinador maneja todo el flujo de EOF.
        Se mantiene por compatibilidad con la interfaz base.
        """
        logger.warning("handle_eof llamado pero ya no se usa (coordinador maneja EOF)")
        return False
    
    def close(self):
        logger.info("Cerrando HourNodeConfigurator...")
        
        # Detener thread de coordinación
        self.coordination_running = False
        if self.coordination_queue:
            try:
                self.coordination_queue.stop_consuming()
                self.coordination_queue.close()
            except Exception as e:
                logger.error(f"Error cerrando coordination_queue: {e}")
        
        if self.coordination_thread and self.coordination_thread.is_alive():
            self.coordination_thread.join(timeout=5)
        
        # Cerrar coordinador
        if self.coordinator:
            self.coordinator.close()
        
        logger.info("HourNodeConfigurator cerrado")