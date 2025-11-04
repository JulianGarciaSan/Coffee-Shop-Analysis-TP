from datetime import datetime
import threading
from logger_writter import LogWriter


class LoggerMonitor:
    """Monitor thread-safe que wrappea LogWriter"""
    
    def __init__(self, log_path):
        self.logger = LogWriter(log_path)
        self.lock = threading.Lock()
    
    def write(self, message):
        """Escritura thread-safe"""
        with self.lock:
            self.logger.write(message)
    
    def write_with_timestamp(self, message):
        """Escritura con timestamp thread-safe"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        with self.lock:
            self.logger.write(f"[{timestamp}] {message}")
    
    def get_last_line(self):
        """Lectura thread-safe de última línea"""
        with self.lock:
            return self.logger.get_last_line()
    
    def _get_last_line(self):
        """Alias para compatibilidad"""
        return self.get_last_line()
    
    def close(self):
        """Cierre thread-safe"""
        with self.lock:
            self.logger.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
