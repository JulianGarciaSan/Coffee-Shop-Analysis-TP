import logging
import threading
from typing import Dict, Set, Optional, Callable
from rabbitmq.middleware import MessageMiddlewareExchange
from dtos.dto import CoordinationMessageDTO

logger = logging.getLogger(__name__)


class PeerCoordinator: 
    def __init__(self, node_id: str, rabbitmq_host: str, total_nodes: int, all_node_ids: list,exchange_name: str = 'coordination_exchange'):
        self.node_id = node_id
        self.rabbitmq_host = rabbitmq_host
        self.total_nodes = total_nodes
        self.all_node_ids = all_node_ids
        
        # Events por cliente: señaliza que llegó EOF_FANOUT
        self.eof_events: Dict[str, threading.Event] = {}
        
        # Flag: ¿ya procesé mi mensaje post-EOF?
        self.eof_processed: Dict[str, bool] = {}
        
        # Timers por cliente (timeout si no hay mensaje pendiente)
        self.ack_timers: Dict[str, threading.Timer] = {}
        
        # Clientes de los cuales soy líder
        self.leader_clients: Set[str] = set()
        
        # ACKs pendientes por cliente (cuando soy líder)
        self.pending_acks: Dict[str, Dict] = {}
        
        # Exchange único para toda la coordinación
        self.coordination_exchange = MessageMiddlewareExchange(
            host=rabbitmq_host,
            exchange_name=exchange_name, 
            route_keys=['coord']
        )
        
        logger.info(f"PeerCoordinator inicializado para {node_id}")
        logger.info(f"  Total nodos: {total_nodes}")
        logger.info(f"  Nodos: {all_node_ids}")
    
    def should_process_message(self, client_id: str) -> bool:
        event = self.eof_events.get(client_id)
        
        if event is None or not event.is_set():
            return True
        
        if self.eof_processed.get(client_id, False):
            return False
        
        # Marcar como procesado y cancelar el timer
        self.eof_processed[client_id] = True
        self._cancel_ack_timer(client_id)
        
        logger.info(f"Procesando último mensaje de cliente {client_id} antes de enviar ACK")
        
        return True
    
    def should_send_ack_after_processing(self, client_id: str) -> bool:
        event = self.eof_events.get(client_id)
        if event and event.is_set() and self.eof_processed.get(client_id, False):
            return True
        return False
    
    def send_ack(self, client_id: str, batch_type: str):
        # Verificar que no hayamos enviado ACK ya
        if self.eof_processed.get(f"{client_id}_ack_sent", False):
            logger.debug(f"ACK ya enviado para cliente {client_id}, ignorando")
            return
        
        self.eof_processed[f"{client_id}_ack_sent"] = True
        
        msg = CoordinationMessageDTO.create_ack(
            client_id=client_id,
            node_id=self.node_id,
            batch_type=batch_type
        )
        
        routing_key = f'coord.{client_id}.ack'
        self.coordination_exchange.send(msg.to_bytes_fast(), routing_key=routing_key)
        
        logger.info(f"ACK enviado para cliente {client_id}")
    
    def _cancel_ack_timer(self, client_id: str):
        if client_id in self.ack_timers:
            self.ack_timers[client_id].cancel()
            del self.ack_timers[client_id]
            logger.debug(f"Timer de ACK cancelado para cliente {client_id}")
    
    def _timeout_ack(self, client_id: str, batch_type: str):
        if not self.eof_processed.get(client_id, False):
            logger.info(f"Timeout ACK para cliente {client_id}: no había mensajes pendientes, enviando ACK")
            self.eof_processed[client_id] = True
            self.send_ack(client_id, batch_type)
        else:
            logger.debug(f"Timer expiró pero mensaje ya procesado para cliente {client_id}")
    
    def take_leadership(self, client_id: str, batch_type: str, on_all_acks_callback):
        logger.info(f"Tomando liderazgo para cliente {client_id}")
        
        self.leader_clients.add(client_id)
        
        other_nodes = [node for node in self.all_node_ids if node != self.node_id]
        self.pending_acks[client_id] = {
            'nodes': set(other_nodes),
            'callback': on_all_acks_callback,
            'batch_type': batch_type
        }
        
        logger.info(f"Esperando ACKs de {len(other_nodes)} nodos: {other_nodes}")
        
        # Hacer fanout del EOF
        msg = CoordinationMessageDTO.create_eof_fanout(
            client_id=client_id,
            node_id=self.node_id,
            batch_type=batch_type
        )
        
        routing_key = f'coord.{client_id}.eof'
        self.coordination_exchange.send(msg.to_bytes_fast(), routing_key=routing_key)
        
        logger.info(f"EOF_FANOUT publicado para cliente {client_id}")
        
        # Si soy el único nodo, llamar callback inmediatamente
        if self.total_nodes == 1:
            logger.info(f"Soy el único nodo, propagando EOF de {client_id} inmediatamente")
            on_all_acks_callback(client_id, batch_type)
            del self.pending_acks[client_id]
            self.leader_clients.discard(client_id)
    
    def handle_ack_received(self, client_id: str, node_id: str, batch_type: str):
        if client_id not in self.leader_clients:
            return
        
        if client_id not in self.pending_acks:
            logger.warning(f"ACK recibido de {node_id} pero no hay pending_acks para {client_id}")
            return
        
        self.pending_acks[client_id]['nodes'].discard(node_id)
        
        pending_count = len(self.pending_acks[client_id]['nodes'])
        logger.info(f"ACK recibido de {node_id} para cliente {client_id}. Pendientes: {pending_count}")
        
        if pending_count == 0:
            logger.info(f"Todos los ACKs recibidos para cliente {client_id}. Propagando EOF downstream")
            
            # Llamar al callback
            callback = self.pending_acks[client_id]['callback']
            batch_type_stored = self.pending_acks[client_id]['batch_type']
            callback(client_id, batch_type_stored)
            
            # Limpiar estado
            del self.pending_acks[client_id]
            self.leader_clients.discard(client_id)
    
    def handle_eof_fanout_received(self, client_id: str, leader_node: str, batch_type: str):
        if leader_node == self.node_id:
            logger.debug(f"Ignorando EOF_FANOUT propio de {client_id}")
            return
        
        logger.info(f"EOF_FANOUT recibido para cliente {client_id} de líder {leader_node}")
        
        event = threading.Event()
        event.set()
        self.eof_events[client_id] = event
        self.eof_processed[client_id] = False
        
        # Iniciar timer de timeout (10 segundos)
        # Si no se procesa ningún mensaje en este tiempo, enviar ACK de todas formas
        timeout_seconds = 10
        timer = threading.Timer(
            timeout_seconds,
            self._timeout_ack,
            args=(client_id, batch_type)
        )
        timer.daemon = True
        timer.start()
        self.ack_timers[client_id] = timer
        
        logger.info(f"Event activado para cliente {client_id}. Timer de {timeout_seconds}s iniciado.")
        logger.info(f"Procesaré hasta 1 mensaje más o enviaré ACK cuando expire el timer.")
    
    def close(self):
        """Cierra las conexiones"""
        logger.info("Cerrando PeerCoordinator...")
        
        # Cancelar todos los timers pendientes
        for client_id, timer in list(self.ack_timers.items()):
            timer.cancel()
            logger.debug(f"Timer cancelado para cliente {client_id}")
        self.ack_timers.clear()
        
        try:
            if self.coordination_exchange:
                self.coordination_exchange.close()
        except Exception as e:
            logger.error(f"Error cerrando exchange de coordinación: {e}")
        
        logger.info("PeerCoordinator cerrado")