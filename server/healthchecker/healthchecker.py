import socket
import threading
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class HealthChecker(threading.Thread):
    def __init__(self, port: int = 9999):
        super().__init__(daemon=False)
        self.port = port
        self.running = True

    def run(self):
        """Servidor TCP simple que responde OK a PING"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
            server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_sock.bind(("", self.port))
            server_sock.listen(5)
            logger.info(f"HealthChecker escuchando en puerto {self.port}")

            while self.running:
                try:
                    conn, addr = server_sock.accept()
                    with conn:
                        data = conn.recv(1024).decode().strip()
                        if data == "PING":
                            conn.sendall(b"OK\n")
                        else:
                            conn.sendall(b"ERR\n")
                except Exception as e:
                    logger.error(f"Error en healthchecker: {e}")

    def stop(self):
        self.running = False

if __name__ == "__main__":
    port = int(os.getenv("HEALTH_PORT", "9999"))
    server = HealthChecker(port)
    server.start()

    try:
        while True:
            pass
    except KeyboardInterrupt:
        server.stop()
