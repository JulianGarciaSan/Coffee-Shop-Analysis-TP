# common/graceful_shutdown.py
import signal
import logging

logger = logging.getLogger(__name__)

class GracefulShutdown:
    """
    Maneja señales SIGTERM/SIGINT para shutdown graceful.
    Cada nodo crea su propia instancia.
    """
    def __init__(self):
        self.shutdown_requested = False
        self._callbacks = []
        
        # Registrar signal handlers
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        
        logger.info("GracefulShutdown inicializado")
    
    def _signal_handler(self, signum, frame):
        """Se ejecuta cuando llega SIGTERM o SIGINT"""
        signal_name = "SIGTERM" if signum == signal.SIGTERM else "SIGINT"
        logger.info(f"Señal {signal_name} recibida - iniciando shutdown")
        
        self.shutdown_requested = True
        
        # Ejecutar callbacks registrados
        for callback in self._callbacks:
            try:
                logger.info(f"Ejecutando callback: {callback.__name__}")
                callback()
            except Exception as e:
                logger.error(f"Error en callback: {e}")

    def register_callback(self, callback):
        """Registra función para ejecutar cuando llegue señal de shutdown"""
        self._callbacks.append(callback)
        logger.info(f"Callback registrado: {callback.__name__}")

    def is_shutting_down(self):
        """Retorna True si se solicitó shutdown"""
        return self.shutdown_requested