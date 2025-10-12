import socket
import threading
from logger import get_logger
from gateway_client import ClientHandler

logger = get_logger(__name__)

class GatewayAcceptor(threading.Thread):
    def __init__(self, port, listener_backlog, shutdown_handler=None,rabbitmq_host=None,input_reports=None,gateway=None,total_join_nodes=1):
        super().__init__(daemon=False)
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listener_backlog)
        self._is_running = False
        self.shutdown = shutdown_handler
        self._clients = []
        self.gateway = gateway
        self.input_reports = input_reports
        self.rabbitmq_host = rabbitmq_host
        self._client_id_counter = 0
        self.total_join_nodes = int(total_join_nodes)
        
    def __accept_new_connection(self):

        logger.info('Accepting new connection')
        c, addr = self._server_socket.accept()
        logger.info(f'Accepted new connection from {addr[0]}')
        return c
    
    
    def start(self):
        self._is_running = True
        try:
            while self._is_running:
                if self.shutdown and self.shutdown.is_shutting_down():
                    logger.info("Shutdown detectado, cerrando gateway")
                    break
                
                try:
                    client_sock = self.__accept_new_connection()
                    client_id = self._client_id_counter
                    client = ClientHandler(client_sock,client_id=client_id,gateway=self.gateway,shutdown_handler=self.shutdown,rabbitmq_host=self.rabbitmq_host,input_reports=self.input_reports,total_join_nodes=self.total_join_nodes)
                    self._client_id_counter += 1
                    self._clients.append(client)
                    client.start()
                    self._reap_dead()
                except socket.timeout:
                    continue
                except Exception as e:
                    if self._is_running:
                        logger.error(f"Error aceptando conexión: {e}")
                    break
                
        except KeyboardInterrupt:
            logger.info("Servidor detenido manualmente")
        finally:
            self._cleanup() 
            
            
    def _reap_dead(self):
        dead_clients = [c for c in self._clients if c.is_dead()]
        
        for client in dead_clients:
            try:
                client.join(timeout=0.1)  
                self._clients.remove(client)
                logger.info(f"Cliente {client.client_id} limpiado de lista")
            except Exception as e:
                logger.error(f"Error limpiando cliente {client.client_id}: {e}")
            
            
    
    def cleanup(self):
        logger.info("Cleanup del GatewayAcceptor...")
        self._is_running = False
        
        try:
            logger.info(f"Deteniendo {len(self._clients)} clientes...")
            for client in self._clients:
                try:
                    client.stop()
                    client.join(timeout=3.0)
                    if client.is_alive():
                            logger.warning(f"Cliente {client.client_id} no terminó")
                except Exception as e:
                    logger.error(f"Error: {e}")
                
                self._clients.clear()
            
            if self._server_socket:
                try:
                    self._server_socket.close()
                    logger.info("Socket cerrado")
                except Exception as e:
                    logger.error(f"Error cerrando socket: {e}")
                
        except Exception as e:
            logger.error(f"Error en cleanup: {e}")
        
        logger.info("GatewayAcceptor cerrado")
        