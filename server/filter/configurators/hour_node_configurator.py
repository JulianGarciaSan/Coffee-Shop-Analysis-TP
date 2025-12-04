# hour_node_configurator.py

import logging
import os
import threading
from typing import Optional, Dict, Any
from rabbitmq.middleware import MessageMiddlewareQueue, MessageMiddlewareExchangeManual, MessageMiddlewareQueueManual
from dtos.dto import TransactionBatchDTO, BatchType, CoordinationMessageDTO
from .base_configurator import NodeConfigurator
from coordinator.coordinator import PeerCoordinator 
from consensus_node import ConsensusNode
logger = logging.getLogger(__name__)


class HourNodeConfigurator(NodeConfigurator):
    def __init__(self, rabbitmq_host: str, logging_instance, client_logging_instance, eof_logging_instance):
        super().__init__(rabbitmq_host, logging_instance, client_logging_instance, eof_logging_instance)
        self.node_id = os.getenv('NODE_ID', f'hour_node_{os.getpid()}')
        self.total_nodes = int(os.getenv('TOTAL_HOUR_FILTERS', '1'))
        all_node_ids_str = os.getenv('ALL_NODE_IDS', self.node_id)
        all_node_ids = [nid.strip() for nid in all_node_ids_str.split(',')]
        
        self.sent_to_q3 = 0
        
        self.leader_id = os.getenv('LEADER_ID', None)
        self.node_addresses_str = os.getenv('NODE_ADDRESSES', '')
        
        nodes_addresses = self._parse_node_addresses(
            self.node_addresses_str,
            all_node_ids
        )              
        self.consensus_node = ConsensusNode(
            node_id=self.node_id,
            leader_id=self.leader_id,
            nodes_addresses=nodes_addresses,
            log_path=f'/app/message_logs.txt',
            max_entries=10
        )
        
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
        
    def _parse_node_addresses(self, addresses_str, node_ids):
        addresses = addresses_str.split(',')
        
        if len(addresses) != len(node_ids):
            raise ValueError(
                f"Mismatch: {len(addresses)} direcciones "
                f"pero {len(node_ids)} node_ids"
            )
        
        nodes_dict = {}
        for i, address in enumerate(addresses):
            host, port = address.split(':')
            node_id = node_ids[i]
            
            nodes_dict[node_id] = (host, int(port))
        
        return nodes_dict
    
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
                self.logger.write_with_timestamp(f"Informo que recibí el FANOUT de EOF para {msg.client_id}")

            
            elif msg.msg_type == CoordinationMessageDTO.ACK:
                self.coordinator.handle_ack_received(
                    msg.client_id,
                    msg.node_id,
                    msg.batch_type_str
                )
                self.logger.write_with_timestamp(f"Informo que recibí el ACK de {msg.node_id} para {msg.client_id}")
            
            else:
                logger.warning(f"Tipo de mensaje desconocido: {msg.msg_type}")
        
        except Exception as e:
            logger.error(f"Error procesando mensaje de coordinación: {e}")
    

    def create_input_middleware(self, input_queue: str, node_id: str):
        logger.info(f"HourNode: Usando working queue compartida '{input_queue}'")
        
        return MessageMiddlewareQueueManual(
            host=self.rabbitmq_host,
            queue_name=input_queue
        )
        
    def create_output_middlewares(self, output_q1: Optional[str], output_q3: Optional[str],
                                  output_q4: Optional[str] = None, output_q2: Optional[str] = None) -> Dict[str, Any]:
        middlewares = {}
        logger.info(f"Configurando middlewares de salida para HourNodeConfigurator {output_q1}, {output_q3}")

        if output_q1:
            middlewares['q1'] = MessageMiddlewareQueueManual(
                host=self.rabbitmq_host,
                queue_name=output_q1
            )
            logger.info(f"  Output Q1 Queue: {output_q1}")
        
        if output_q3:
            middlewares['q3'] = MessageMiddlewareExchangeManual(
                host=self.rabbitmq_host,
                exchange_name=output_q3,
                route_keys=['semester.1', 'semester.2', 'eof.all'],
                queue_name=f'filter.hour.node.{self.node_id}.ouputq3'
            )
            logger.info(f"  Output Q3 Exchange: {output_q3}")
        
        self.output_middlewares = middlewares
        
        return middlewares
    
    def process_filtered_data(self, filtered_csv: str) -> str:
        return filtered_csv

    def process_message(self, body: bytes, routing_key: str = None, client_id: Optional[int] = None, message_id: Optional[int] = None) -> tuple:
        decoded_data = body.decode('utf-8').strip()
        
        client_id_str = str(client_id) if client_id is not None else "default"
        message_id_str = str(message_id) if message_id is not None else "default"
        
        if decoded_data.startswith("EOF:"):
            if decoded_data.startswith("EOF:1"):
                logger.info(f"EOF recibido para cliente {client_id_str}")
                self.logger.write_with_timestamp(f"EOF:{client_id_str}")
                self.eof_logger.write(f"BEF:{client_id_str}:transactions")

                self.coordinator.take_leadership(
                    client_id_str, 
                    message_id_str,
                    'transactions',
                    self._on_all_acks_received
                )
            elif decoded_data.startswith("EOF:2") or decoded_data.startswith("EOF:3"):
                if decoded_data.startswith("EOF:2"):
                    logger.info(f"EOF:2 recibido para cliente {client_id_str}")
                    eof_type = 2
                else:
                    logger.info(f"EOF:3 recibido, formateando nodos")
                    eof_type = 3
                    
                self.eof_logger.write(f"EOF:2:{client_id_str}:transactions")
                self.send_eof(self.output_middlewares, "transactions", client_id,message_id,eof_type=eof_type)

            dto = TransactionBatchDTO(decoded_data, BatchType.EOF)
            return (False, 'transactions', dto, False)
        
        if not self.coordinator.should_process_message(client_id_str):
            logger.info(f"Cliente {client_id_str} ya finalizó, ignorando mensaje")
            dto = TransactionBatchDTO(decoded_data, BatchType.RAW_CSV)
            return (True, 'transactions', dto, False)
        
        dto = TransactionBatchDTO(decoded_data, BatchType.RAW_CSV)
        return (False, 'transactions', dto, False)

    def send_data(self, data: str, middlewares: Dict[str, Any], batch_type: str = "transactions", client_id: Optional[int] = None,message_id:Optional[int]=None):
        headers = self.create_headers(client_id,message_id)
        
        if client_id:
            client_id_str = str(client_id)
            if self.coordinator.should_send_ack_after_processing(client_id_str):
                self.coordinator.send_ack(client_id_str, 'transactions')
        
        if 'q1' in middlewares:
            filtered_dto = TransactionBatchDTO(data, batch_type=BatchType.RAW_CSV)
            middlewares['q1'].send(filtered_dto.to_bytes_fast(), headers=headers)
        
        if 'q3' in middlewares:
            self._send_to_exchange_by_semester(data, middlewares['q3'], client_id,message_id)

    def _on_all_acks_received(self, client_id: str,message_id:str, batch_type: str):
        logger.info(f"Todos los ACKs recibidos para cliente {client_id}, propagando EOF downstream")
        self.eof_logger.write(f"EOF:{client_id}:{batch_type}")

        if self.output_middlewares is None:
            logger.error("output_middlewares no está configurado")
            return
        
        client_id_int = int(client_id) if client_id.isdigit() else None
        message_id_int = int(message_id) if message_id.isdigit() else None
        self.send_eof(self.output_middlewares, "transactions", client_id=client_id_int,message_id=message_id_int)
        self.eof_logger.write(f"END:{client_id}:{batch_type}")

    def send_eof(self, middlewares: Dict[str, Any], batch_type: str = "transactions", client_id: Optional[int] = None,message_id:Optional[int]=None, eof_type: Optional[int] = 1):
        if eof_type == 2 or eof_type == 3:
            headers = self.create_headers(client_id,0)
            eof_dto = TransactionBatchDTO(f"EOF:{eof_type}", BatchType.EOF)
        elif eof_type == 1:
            headers = self.create_headers(client_id,message_id)
            eof_dto = TransactionBatchDTO("EOF:1", BatchType.EOF)

        logger.info(f"Enviando EOF de tipo {eof_type} para cliente {client_id}")

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
            
    def _send_to_exchange_by_semester(self, csv_data: str, exchange_middleware, client_id: Optional[int] = None,message_id:Optional[int]=None):
        headers = self.create_headers(client_id,message_id)
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
        if len(fields) >= 5:
            date_str = fields[4]
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
        
        self.coordination_running = False
        if self.coordination_queue:
            try:
                self.coordination_queue.stop_consuming()
                self.coordination_queue.close()
            except Exception as e:
                logger.error(f"Error cerrando coordination_queue: {e}")
        
        if self.coordination_thread and self.coordination_thread.is_alive():
            self.coordination_thread.join(timeout=5)
        
        if self.coordinator:
            self.coordinator.close()
        
        logger.info("HourNodeConfigurator cerrado")