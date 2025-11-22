import os
import traceback


class LogWriter:
    def __init__(self, log_path='/app/logs.txt'):
        self.log_path = log_path
        self.file = None
        self.last_line = ""
        print(f"[LogWriter] Inicializando con path: {log_path}")
        self._initialize()
    
    def _initialize(self):
        """Abre el archivo en modo append y lee solo la última línea"""
        print(f"[LogWriter] Verificando existencia de archivo: {self.log_path}")
        
        if not os.path.exists(self.log_path):
            print(f"[LogWriter] Archivo no existe, creando: {self.log_path}")
            # Crear directorio si no existe
            #os.makedirs(os.path.dirname(self.log_path), exist_ok=True)
            open(self.log_path, 'a').close()
        else:
            print(f"[LogWriter] Archivo existe: {self.log_path}")
        
        # Verificar permisos
        if os.access(self.log_path, os.R_OK):
            print(f"[LogWriter] Tengo permisos de lectura")
        else:
            print(f"[LogWriter] NO tengo permisos de lectura")
        
        if os.access(self.log_path, os.W_OK):
            print(f"[LogWriter] Tengo permisos de escritura")
        else:
            print(f"[LogWriter] NO tengo permisos de escritura")
        
        print(f"[LogWriter] Leyendo última línea...")
        self.last_line = self._get_last_line()
        print(f"[LogWriter] Última línea leída: '{self.last_line}'")
        
        print(f"[LogWriter] Abriendo archivo en modo append...")
        self.file = open(self.log_path, 'a', buffering=1)
        print(f"[LogWriter] Archivo abierto correctamente")
    
    def _get_last_line(self):
        """Lee solo la última línea del archivo sin cargar todo en memoria"""
        try:
            # print(f"[LogWriter._get_last_line] Abriendo archivo: {self.log_path}")
            
            with open(self.log_path, 'rb') as f:
                f.seek(0, os.SEEK_END)
                file_size = f.tell()
                # print(f"[LogWriter._get_last_line] Tamaño del archivo: {file_size} bytes")
                
                if file_size == 0:
                    # print(f"[LogWriter._get_last_line] Archivo vacío")
                    return ""
                
                buffer_size = min(8192, file_size)
                # print(f"[LogWriter._get_last_line] Buffer size: {buffer_size} bytes")
                
                f.seek(-buffer_size, os.SEEK_END)
                content = f.read()
                # print(f"[LogWriter._get_last_line] Bytes leídos: {len(content)}")
                
                decoded = content.decode('utf-8', errors='ignore')
                # print(f"[LogWriter._get_last_line] Contenido decodificado: {len(decoded)} caracteres")
                
                lines = decoded.splitlines()
                # print(f"[LogWriter._get_last_line] Líneas encontradas: {len(lines)}")
                
                if lines:
                    last = lines[-1]
                    # print(f"[LogWriter._get_last_line] Última línea: '{last[:100]}'...")  # Primeros 100 chars
                    return last
                else:
                    # print(f"[LogWriter._get_last_line] No se encontraron líneas después de split")
                    return ""
                    
        except Exception as e:
            # print(f"[LogWriter._get_last_line] Error: {e}")
            traceback.print_exc()
            return ""
    
    def write(self, message):
        """Escribe una línea en el log"""
        if self.file and not self.file.closed:
            # print(f"[LogWriter.write] Escribiendo: '{message[:100]}'...")
            self.file.write(f"\n{message}")
            self.file.flush()
            os.fsync(self.file.fileno())
            self.last_line = message
            # print(f"[LogWriter.write] Escrito y flusheado")
        else:
            error_msg = "El archivo de log está cerrado"
            # print(f"[LogWriter.write] {error_msg}")
            raise Exception(error_msg)
    
    def get_last_line(self):
        """Retorna la última línea leída/escrita"""
        # print(f"[LogWriter.get_last_line] Retornando: '{self.last_line[:100] if self.last_line else '(vacío)'}'...")
        return self.last_line
    
    def close(self):
        """Cierra el archivo"""
        # print(f"[LogWriter.close] Cerrando archivo...")
        if self.file and not self.file.closed:
            self.file.close()
            # print(f"[LogWriter.close] Archivo cerrado")
        else:
            print(f"[LogWriter.close] Archivo ya estaba cerrado")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()