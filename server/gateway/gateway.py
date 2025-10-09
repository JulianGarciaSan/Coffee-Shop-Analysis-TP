from asyncio import Lock
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
from dtos.dto import TransactionBatchDTO, BatchType, StoreBatchDTO,UserBatchDTO, ReportBatchDTO, TransactionItemBatchDTO, MenuItemBatchDTO
from gateway_acceptor import GatewayAcceptor

logger = get_logger(__name__)

class Gateway:

    def __init__(self, port, listener_backlog, rabbitmq_host, output_year_node_exchange, output_join_node, input_reports=None, shutdown_handler=None):
        self.shutdown = shutdown_handler
        
        #self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #self._server_socket.bind(('', port))
        #self._server_socket.listen(listener_backlog)
        self._acceptor = GatewayAcceptor(port, listener_backlog, shutdown_handler, rabbitmq_host,input_reports,self)

        self._is_running = False
        self.rabbitmq_host = rabbitmq_host
        self.output_year_node_exchange = output_year_node_exchange
        self.output_join_node = output_join_node
        self.output_filter_year_nodes_middleware = None
        self._join_middleware = None
        self.setup_common_middleware()

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

    def get_output_middleware(self):
        return self.output_filter_year_nodes_middleware

    def get_join_middleware(self):
        return self._join_middleware

    def start(self):
        self._is_running = True
        logger.info("Iniciando Gateway...")
        
        try:
            self._acceptor.start()  
        except Exception as e:
            logger.error(f"Error en el Gateway: {e}")
            raise
        finally:
            self._cleanup()

    def _cleanup(self):
        logger.info("Iniciando cleanup del Gateway...")
        self._is_running = False
        
        try:
            if self._acceptor:
                self._acceptor.cleanup()
                self._acceptor.join()
            
            if self.output_filter_year_nodes_middleware:
                self.output_filter_year_nodes_middleware.close()
            if self._join_middleware:
                self._join_middleware.close()
                
        except Exception as e:
            logger.error(f"Error durante cleanup: {e}")
        
        logger.info("Gateway cerrado completamente")
            

