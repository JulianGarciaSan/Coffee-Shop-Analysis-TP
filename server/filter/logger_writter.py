import os

class LogWriter:
    def __init__(self, log_path='/app/logs.txt'):
        self.log_path = log_path
        self.file = None
        self._initialize()
        self.last_line = ""
    
    def _initialize(self):
        """Abre el archivo en modo append y lee solo la última línea"""
        if not os.path.exists(self.log_path):
            open(self.log_path, 'a').close()
        
        self.last_line = self._get_last_line()
        
        self.file = open(self.log_path, 'a', buffering=1)  
    
    def _get_last_line(self):
        """Lee solo la última línea del archivo sin cargar todo en memoria"""
        try:
            with open(self.log_path, 'rb') as f:
                f.seek(0, os.SEEK_END)
                file_size = f.tell()
                
                if file_size == 0:
                    return ""
                
                buffer_size = min(8192, file_size)
                f.seek(-buffer_size, os.SEEK_END)
                lines = f.read().decode('utf-8', errors='ignore').splitlines()
                
                return lines[-1] if lines else ""
        except Exception as e:
            print(f"Error leyendo última línea: {e}")
            return ""
    
    def write(self, message):
        """Escribe una línea en el log"""
        if self.file and not self.file.closed:
            self.file.write(f"\n{message}")
            self.file.flush()  
            self.last_line = message
        else:
            raise Exception("El archivo de log está cerrado")
    
    def get_last_line(self):
        """Retorna la última línea leída/escrita"""
        return self.last_line
    
    def close(self):
        """Cierra el archivo"""
        if self.file and not self.file.closed:
            self.file.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

