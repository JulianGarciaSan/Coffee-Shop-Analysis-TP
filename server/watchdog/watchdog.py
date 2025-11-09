import socket
import time
import docker
import os
import logging
from typing import Dict

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class NodeWatchdog:
    def __init__(self):
        self.client = docker.from_env()
        self.check_interval = int(os.getenv("CHECK_INTERVAL", "10"))
        self.max_failures = int(os.getenv("MAX_FAILURES", "3"))
        self.startup_delay = int(os.getenv("STARTUP_DELAY", "10"))
        self.ping_timeout = float(os.getenv("PING_TIMEOUT", "2.0"))

        monitored_str = os.getenv("MONITORED_NODES", "")
        self.monitored_nodes: Dict[str, int] = {}
        for node_spec in monitored_str.split(","):
            if ":" in node_spec:
                name, port = node_spec.split(":")
                self.monitored_nodes[name.strip()] = int(port)

        self.failure_counts = {name: 0 for name in self.monitored_nodes}

        logger.info("Watchdog inicializado")
        logger.info(f"Nodos monitoreados: {self.monitored_nodes}")

    def ping_node(self, container_name: str, port: int) -> bool:
        """Intenta conectar por TCP al nodo y esperar respuesta 'OK'"""
        try:
            with socket.create_connection((container_name, port), timeout=self.ping_timeout) as sock:
                sock.sendall(b"PING\n")
                resp = sock.recv(1024).decode().strip()
                if resp == "OK":
                    return True
                logger.warning(f"{container_name}:{port} respondio {resp}")
                return False
        except (socket.timeout, ConnectionRefusedError):
            logger.warning(f"{container_name}:{port} no responde")
            return False
        except Exception as e:
            logger.error(f"Error al pingear {container_name}:{port}: {e}")
            return False

    def restart_container(self, container_name: str):
        """Reinicia el contenedor fallido"""
        try:
            logger.warning(f"Reiniciando container {container_name}...")
            container = self.client.containers.get(container_name)
            container.restart(timeout=10)
            self.failure_counts[container_name] = 0
            logger.info(f"{container_name} reiniciado exitosamente")
        except Exception as e:
            logger.error(f"Error reiniciando {container_name}: {e}")

    def check_nodes(self):
        for name, port in self.monitored_nodes.items():
            if self.ping_node(name, port):
                self.failure_counts[name] = 0
                logger.debug(f"{name}:{port} OK")
            else:
                self.failure_counts[name] += 1
                logger.warning(f"{name} fallo {self.failure_counts[name]}/{self.max_failures}")
                if self.failure_counts[name] >= self.max_failures:
                    self.restart_container(name)

    def run(self):
        logger.info("Watchdog iniciado")
        time.sleep(self.startup_delay)
        while True:
            logger.info("Iniciando nuevo ciclo de chequeo...")
            self.check_nodes()
            time.sleep(self.check_interval)


if __name__ == "__main__":
    NodeWatchdog().run()
