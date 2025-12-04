from asyncio import Lock
import threading
import time
import socket
import os
import json
import signal
import sys
from common.protocol import Protocol, ProtocolMessage 
from common.new_protocolo import ProtocolNew
from rabbitmq.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from dataclasses import asdict
from logger import get_logger
from dtos.dto import BatchType, MenuItemBatchDTO, ReportBatchDTO, StoreBatchDTO, TransactionBatchDTO, TransactionItemBatchDTO, UserBatchDTO
from gateway_acceptor import GatewayAcceptor
from handler_report import ReportHandler

logger = get_logger(__name__)

class Gateway:

    def __init__(self, port, listener_backlog, rabbitmq_host, output_year_node_exchange, output_join_node, input_reports=None, shutdown_handler=None,total_join_nodes=1):
        self.shutdown = shutdown_handler
        if self.shutdown:
            self.shutdown.register_callback(self._on_shutdown_signal)
        
        self._acceptor = GatewayAcceptor(port, listener_backlog, shutdown_handler, rabbitmq_host,input_reports,self, total_join_nodes)

        self._is_running = False
        self.rabbitmq_host = rabbitmq_host
        self.output_year_node_exchange = output_year_node_exchange
        self.output_join_node = output_join_node
        self.output_filter_year_nodes_middleware = None
        self._join_middleware = None
        self._reports_exchange = input_reports
        self._report_handler = None
        self._clients_lock = threading.Lock()
        self._clients_by_id = {}
        self.setup_common_middleware()

    def _on_shutdown_signal(self):
        """Callback ejecutado cuando llega SIGTERM/SIGINT"""
        logger.info("Señal de shutdown recibida en Gateway")
        self._is_running = False
        
        if self._acceptor:
            self._acceptor.stop()

    def setup_common_middleware(self):
        self.output_filter_year_nodes_middleware = MessageMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name=self.output_year_node_exchange,
            route_keys=['transactions', 'transaction_items']
        )
        
        self._join_middleware = MessageMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name=self.output_join_node,
            route_keys=['stores.data', 'users.data', 'users.eof', 'menu_items.data']
        )
        
        if self.shutdown:
            if hasattr(self.output_filter_year_nodes_middleware, 'shutdown'):
                self.output_filter_year_nodes_middleware.shutdown = self.shutdown
            if hasattr(self._join_middleware, 'shutdown'):
                self._join_middleware.shutdown = self.shutdown
                
        if self._reports_exchange:
            self._start_report_handler()

    def _start_report_handler(self):
        if self._report_handler:
            return
        self._report_handler = ReportHandler(
            rabbitmq_host=self.rabbitmq_host,
            reports_exchange=self._reports_exchange,
            gateway=self,
            shutdown_handler=self.shutdown,
        )
        self._report_handler.start()
        
    def register_client(self, client_handler):
        with self._clients_lock:
            self._clients_by_id[client_handler.client_id] = client_handler
            logger.info(f"Cliente {client_handler.client_id} registrado en Gateway")

    def unregister_client(self, client_id):
        with self._clients_lock:
            if client_id in self._clients_by_id:
                del self._clients_by_id[client_id]
                logger.info(f"Cliente {client_id} removido del Gateway")

    def get_output_middleware(self):
        mw = MessageMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name=self.output_year_node_exchange,
            route_keys=['transactions', 'transaction_items']
        )
        if self.shutdown and hasattr(mw, 'shutdown'):
            mw.shutdown = self.shutdown
        return mw

    def get_join_middleware(self):
        mw = MessageMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name=self.output_join_node,
            route_keys=['stores.data', 'users.data', 'users.eof', 'menu_items.data']
        )
        if self.shutdown and hasattr(mw, 'shutdown'):
            mw.shutdown = self.shutdown
        return mw
    
    def dispatch_report_to_client(self, client_id, routing_key, body):
        # print(f"Dispatching report to client {client_id} with routing_key={routing_key}")
        with self._clients_lock:
            client_handler = self._clients_by_id.get(client_id)

        if not client_handler:
            logger.warning(f"No se encontró handler para cliente {client_id}, descartando reporte")
            return
        client_handler.enqueue_report((routing_key, body))

    def start(self):
        self._is_running = True
        logger.info("Iniciando Gateway...")
        
        try:
            self._send_initial_cleanup()
            self._acceptor.start()  
        except Exception as e:
            logger.error(f"Error en el Gateway: {e}")
            raise
        finally:
            self._cleanup()
            
    def _send_initial_cleanup(self):
        """Envía EOF:3 al iniciar para limpiar datos residuales"""
        logger.info("Enviando EOF:3 inicial para cleanup de datos residuales")
        
        try:
            # Crear DTOs con EOF:3
            eof_transactions = TransactionBatchDTO("EOF:3", batch_type=BatchType.EOF)
            eof_items = TransactionItemBatchDTO("EOF:3", batch_type=BatchType.EOF)
            eof_stores = StoreBatchDTO("EOF:3", batch_type=BatchType.EOF)
            eof_users = UserBatchDTO("EOF:3", batch_type=BatchType.EOF)
            eof_menu = MenuItemBatchDTO("EOF:3", batch_type=BatchType.EOF)
            
            headers = {'client_id': -1, 'message_id': 0}
            
            self.output_filter_year_nodes_middleware.send(
                eof_transactions.to_bytes_fast(),
                routing_key='transactions',
                headers=headers
            )
            self.output_filter_year_nodes_middleware.send(
                eof_items.to_bytes_fast(),
                routing_key='transaction_items',
                headers=headers
            )
            
            for node_id in range(self._acceptor.total_join_nodes):
                self._join_middleware.send(
                    eof_stores.to_bytes_fast(),
                    routing_key=f'join_node_{node_id}.stores.data',
                    headers=headers
                )
                self._join_middleware.send(
                    eof_users.to_bytes_fast(),
                    routing_key=f'join_node_{node_id}.users.data',
                    headers=headers
                )
                self._join_middleware.send(
                    eof_menu.to_bytes_fast(),
                    routing_key=f'join_node_{node_id}.menu_items.data',
                    headers=headers
                )
            
            logger.info("-------------- EOF:3 INICIAL ENVIADO EXITOSAMENTE  --------------------")
            
        except Exception as e:
            logger.error(f"----------------- Error enviando EOF:3 inicial: {e} -----------------------")

    def _cleanup(self):
        logger.info("Iniciando cleanup del Gateway...")
        self._is_running = False
        
        try:
            if self._acceptor:
                logger.info("Cerrando acceptor...")
                self._acceptor.cleanup()
                self._acceptor.join(timeout=5.0)
                if self._acceptor.is_alive():
                    logger.warning("Acceptor no terminó en el tiempo esperado")
            
            time.sleep(0.5)            
            if self.output_filter_year_nodes_middleware:
                logger.info("Cerrando middleware de output...")
                self.output_filter_year_nodes_middleware.close()
                
            if self._join_middleware:
                logger.info("Cerrando middleware de join...")
                self._join_middleware.close()
            if self._report_handler:
                try:
                    self._report_handler.stop()
                    self._report_handler.join(timeout=3.0)
                except Exception as e:
                    logger.error(f"Error deteniendo report handler: {e}")
                
        except Exception as e:
            logger.error(f"Error durante cleanup: {e}")
        
        logger.info("Gateway cerrado completamente")