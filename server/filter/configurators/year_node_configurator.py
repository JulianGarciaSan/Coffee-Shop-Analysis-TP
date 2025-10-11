import logging
import os
import threading
from typing import Optional, Dict, Any
from rabbitmq.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from dtos.dto import TransactionBatchDTO, TransactionItemBatchDTO, BatchType, CoordinationMessageDTO
from .base_configurator import NodeConfigurator
from coordinator.coordinator import PeerCoordinator 
logger = logging.getLogger(__name__)


class YearNodeConfigurator(NodeConfigurator):
    def __init__(self, rabbitmq_host: str):
        super().__init__(rabbitmq_host)
        self.file_mode = os.getenv('FILE_MODE', 'transactions')
        

        self.node_id = os.getenv('NODE_ID', f'year_node_{os.getpid()}')
        self.total_nodes = int(os.getenv('TOTAL_YEAR_FILTERS', '1'))
        all_node_ids_str = os.getenv('ALL_NODE_IDS', self.node_id)
        all_node_ids = [nid.strip() for nid in all_node_ids_str.split(',')]
        
        # Crear coordinador (helper)
        self.coordinator = PeerCoordinator(
            node_id=self.node_id,
            rabbitmq_host=rabbitmq_host,
            total_nodes=self.total_nodes,
            all_node_ids=all_node_ids,
            exchange_name='coordination_year_exchange' 
        )

        
        # Middleware de coordinación
        self.coordination_queue = MessageMiddlewareQueue(
            host=rabbitmq_host,
            queue_name=f'coordination_{self.node_id}',
            exchange_name='coordination_year_exchange', 
            routing_keys=['coord.#']
        )
        
        # Thread de coordinación
        self.coordination_thread: Optional[threading.Thread] = None
        self.coordination_running = False
        
        # Referencias que se setean después
        self.output_middlewares: Optional[Dict[str, Any]] = None
        
        # Iniciar thread de coordinación
        self._start_coordination_thread()
        
        logger.info(f"YearNodeConfigurator inicializado con coordinación multi-cliente")
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
    

    def create_input_middleware(self, input_exchange: str, input_queue: str):
        if self.file_mode == 'transactions':
            queue_name = 'year_transactions_queue'
            routing_key = 'transactions'
        else:  
            queue_name = 'year_items_queue'
            routing_key = 'transaction_items'
        
        logger.info(f"YearNode: Queue={queue_name}, routing_key={routing_key}")
        
        return MessageMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=queue_name,
            exchange_name=input_exchange,
            routing_keys=[routing_key]
        )
    
    def process_message(self, body: bytes, routing_key: str = None, client_id: Optional[int] = None) -> tuple:
        decoded_data = body.decode('utf-8').strip()
        
        client_id_str = str(client_id) if client_id is not None else "default"
        
        # Manejar EOF
        if decoded_data.startswith("EOF:"):
            logger.info(f"EOF recibido para cliente {client_id_str}")
            
            # Tomar liderazgo
            batch_type = 'transactions' if self.file_mode == 'transactions' else 'transaction_items'
            self.coordinator.take_leadership(
                client_id_str, 
                batch_type,
                self._on_all_acks_received  # Pasamos el callback
            )
            
            # Retornar should_stop=False para que main.py NO lo trate como EOF
            if self.file_mode == 'transactions':
                dto = TransactionBatchDTO(decoded_data, BatchType.EOF)
            else:
                dto = TransactionItemBatchDTO(decoded_data, BatchType.EOF)
            
            return (False, batch_type, dto, False)
        
        # Verificar si debo procesar este mensaje (coordinación)
        if not self.coordinator.should_process_message(client_id_str):
            logger.info(f"Cliente {client_id_str} ya finalizó, ignorando mensaje")
            # Retornar should_stop=True para que main.py no procese
            if self.file_mode == 'transactions':
                dto = TransactionBatchDTO(decoded_data, BatchType.RAW_CSV)
                return (True, 'transactions', dto, False)
            else:
                dto = TransactionItemBatchDTO(decoded_data, BatchType.RAW_CSV)
                return (True, 'transaction_items', dto, False)
        
        # Procesar mensaje normalmente
        if self.file_mode == 'transactions':
            logger.debug(f"YearNode: Mensaje recibido para 'transactions', tamaño {len(decoded_data)} bytes")
            dto = TransactionBatchDTO(decoded_data, BatchType.RAW_CSV)
            return (False, 'transactions', dto, False)
        else:
            dto = TransactionItemBatchDTO(decoded_data, BatchType.RAW_CSV)
            return (False, 'transaction_items', dto, False)
    
    def create_output_middlewares(self, output_q1: Optional[str], output_q3: Optional[str],
                                  output_q4: Optional[str] = None, output_q2: Optional[str] = None) -> Dict[str, Any]:
        middlewares = {}

        if output_q1:
            middlewares['q1'] = MessageMiddlewareQueue(
                host=self.rabbitmq_host,
                queue_name=output_q1
            )
            logger.info(f"  Output Q1 Queue: {output_q1}")
        
        if output_q2:
            middlewares['q2'] = MessageMiddlewareExchange(
                host=self.rabbitmq_host,
                exchange_name=output_q2,
                route_keys=['2024', '2025']
            )
            logger.info(f"  Output Q2 Exchange: {output_q2}")
        
        if output_q4:
            middlewares['q4'] = MessageMiddlewareQueue(
                host=self.rabbitmq_host,
                queue_name=output_q4
            )
            logger.info(f"  Output Q4 Queue: {output_q4}")
        
        # Guardar referencia para usar en callbacks
        self.output_middlewares = middlewares
        
        return middlewares
    
    def process_filtered_data(self, filtered_csv: str) -> str:
        return filtered_csv
    
    def send_data(self, data: str, middlewares: Dict[str, Any], batch_type: str = "transactions", client_id: Optional[int] = None):
        headers = self.create_headers(client_id)
        
        # Verificar si debo enviar ACK después de enviar datos
        if client_id:
            client_id_str = str(client_id)
            if self.coordinator.should_send_ack_after_processing(client_id_str):
                batch_type_str = 'transactions' if batch_type == 'transactions' else 'transaction_items'
                self.coordinator.send_ack(client_id_str, batch_type_str)
        
        if batch_type == "transactions":
            filtered_dto = TransactionBatchDTO(data, batch_type=BatchType.RAW_CSV)
            
            if 'q1' in middlewares:
                middlewares['q1'].send(filtered_dto.to_bytes_fast(), headers=headers)
            
            if 'q4' in middlewares:
                middlewares['q4'].send(filtered_dto.to_bytes_fast(), headers=headers)
        
        elif batch_type == "transaction_items":
            logger.info(f"Procesando líneas de TransactionItems para Q2")
            if 'q2' in middlewares:
                self._send_transaction_items_by_year(data, middlewares['q2'], client_id)

    def _on_all_acks_received(self, client_id: str, batch_type: str):
        logger.info(f"Todos los ACKs recibidos para cliente {client_id}, propagando EOF downstream")
        
        if self.output_middlewares is None:
            logger.error("output_middlewares no está configurado")
            return
        
        client_id_int = int(client_id) if client_id.isdigit() else None
        
        if batch_type == 'transactions':
            self.send_eof(self.output_middlewares, "transactions", client_id=client_id_int)
        else:
            self.send_eof(self.output_middlewares, "transaction_items", client_id=client_id_int)

    def send_eof(self, middlewares: Dict[str, Any], batch_type: str = "transactions", client_id: Optional[int] = None):
        headers = self.create_headers(client_id)
        
        if batch_type == "transactions":
            eof_dto = TransactionBatchDTO("EOF:1", batch_type=BatchType.EOF)
            
            if 'q1' in middlewares:
                middlewares['q1'].send(eof_dto.to_bytes_fast(), headers=headers)
                logger.info(f"EOF enviado a Q1 para cliente {client_id}")
            
            if 'q4' in middlewares:
                middlewares['q4'].send(eof_dto.to_bytes_fast(), headers=headers)
                logger.info(f"EOF enviado a Q4 para cliente {client_id}")
        
        elif batch_type == "transaction_items":
            if 'q2' in middlewares:
                eof_dto = TransactionItemBatchDTO("EOF:1", batch_type=BatchType.EOF)
                middlewares['q2'].send(eof_dto.to_bytes_fast(), routing_key='2024', headers=headers)
                middlewares['q2'].send(eof_dto.to_bytes_fast(), routing_key='2025', headers=headers)
                logger.info(f"EOF enviado a Q2 para cliente {client_id}")
    
    def handle_eof(self, counter: int, total_filters: int, eof_type: str, 
            middlewares: Dict[str, Any], input_middleware: Any, client_id: Optional[int] = None) -> bool:
        """
        Ya NO se usa - el coordinador maneja todo el flujo de EOF.
        Se mantiene por compatibilidad con la interfaz base.
        """
        logger.warning("handle_eof llamado pero ya no se usa (coordinador maneja EOF)")
        return False
    
    def _send_transaction_items_by_year(self, data: str, q2_middleware, client_id: Optional[int] = None):
        headers = self.create_headers(client_id)
        lines = data.strip().split('\n')
        header = lines[0] if lines else ""
        
        logger.info(f"Procesando {len(lines)} líneas de TransactionItems para Q2")
        
        data_by_year = {'2024': [header], '2025': [header]}
        dto_helper = TransactionItemBatchDTO("", BatchType.RAW_CSV)
        
        for line in lines[1:]:
            if not line.strip():
                continue
                
            try:
                created_at = dto_helper.get_column_value(line, 'created_at')
                if created_at and len(created_at) >= 4:
                    year = created_at[:4]
                    if year in data_by_year:
                        data_by_year[year].append(line)
                else:
                    logger.warning(f"Created_at inválido: '{created_at}' en línea: {line[:50]}...")
            except Exception as e:
                logger.warning(f"Error procesando línea TransactionItem para Q2: {e}")
                continue
        
        for year, year_lines in data_by_year.items():
            if len(year_lines) > 1:
                year_csv = '\n'.join(year_lines)
                year_dto = TransactionItemBatchDTO(year_csv, batch_type=BatchType.RAW_CSV)
                q2_middleware.send(year_dto.to_bytes_fast(), routing_key=year, headers=headers)
                logger.info(f"TransactionItemBatchDTO enviado a Q2 con routing key {year}: {len(year_lines)-1} líneas")
            else:
                logger.info(f"No hay datos para año {year} - solo header")
    
    def close(self):
        """Cleanup del configurator"""
        logger.info("Cerrando YearNodeConfigurator...")
        
        # Detener thread de coordinación
        self.coordination_running = False
        if self.coordination_queue:
            try:
                self.coordination_queue.stop_consuming()
                self.coordination_queue.close()
            except Exception as e:
                logger.error(f"Error cerrando coordination_queue: {e}")
        
        if self.coordination_thread and self.coordination_thread.is_alive():
            self.coordination_thre