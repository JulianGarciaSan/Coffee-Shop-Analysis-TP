import time
import docker
import os
import logging
from typing import Dict, Set
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class NodeWatchdog:
    def __init__(self):
        self.client = docker.from_env()
        self.check_interval = int(os.getenv("CHECK_INTERVAL", "2"))
        self.max_failures = int(os.getenv("MAX_FAILURES", "4"))
        self.startup_delay = int(os.getenv("STARTUP_DELAY", "10"))
        
        self.restart_grace_period = int(os.getenv("RESTART_GRACE_PERIOD", "30"))
        self.restart_check_interval = int(os.getenv("RESTART_CHECK_INTERVAL", "10"))

        monitored_str = os.getenv("MONITORED_NODES", "")
        self.monitored_nodes: Set[str] = {
            name.strip() for name in monitored_str.split(",") if name.strip()
        }

        self.failure_counts = {name: 0 for name in self.monitored_nodes}
        self.last_restart_time: Dict[str, datetime] = {}
        self.ignored_nodes: Set[str] = set()

        logger.info("Watchdog inicializado")
        logger.info(f"Nodos monitoreados: {self.monitored_nodes}")
        logger.info(f"Grace period post-restart: {self.restart_grace_period}s")

    def check_container_health(self, container_name: str) -> bool:
        """
        Verifica si el contenedor está saludable.
        Returns True si está OK, False si tiene problemas.
        """
        try:
            container = self.client.containers.get(container_name)
            status = container.status
            
            if status == 'running':
                health = container.attrs.get('State', {}).get('Health', {})
                if health:
                    health_status = health.get('Status', 'none')
                    if health_status == 'healthy':
                        logger.debug(f"{container_name}: running + healthy")
                        return True
                    elif health_status == 'starting':
                        logger.info(f"{container_name}: starting (tolerado)")
                        return True  
                    elif health_status == 'unhealthy':
                        logger.warning(f"{container_name}: unhealthy")
                        return False
                else:
                    logger.debug(f"{container_name}: running (sin healthcheck)")
                    return True
            
            elif status == 'restarting':
                logger.info(f"{container_name}: restarting (tolerado)")
                return True
            
            elif status == 'exited':
                exit_code = container.attrs.get('State', {}).get('ExitCode', -1)
                logger.warning(f"{container_name}: exited (code {exit_code})")
                return False
            
            elif status == 'paused':
                logger.warning(f"{container_name}: paused")
                return False
            
            elif status == 'dead':
                logger.error(f"{container_name}: dead")
                return False
            
            else:
                logger.warning(f"{container_name}: estado desconocido '{status}'")
                return False
                
        except docker.errors.NotFound:
            logger.warning(f"{container_name}: no existe")
            return False
        except Exception as e:
            logger.error(f"Error chequeando {container_name}: {e}")
            return False

    def is_in_grace_period(self, container_name: str) -> bool:
        """Verifica si el nodo está en grace period post-restart"""
        if container_name not in self.last_restart_time:
            return False
        
        elapsed = (datetime.now() - self.last_restart_time[container_name]).total_seconds()
        in_grace = elapsed < self.restart_grace_period
        
        if in_grace:
            remaining = self.restart_grace_period - elapsed
            logger.info(f"{container_name} en GRACE PERIOD - {remaining:.0f}s restantes")
        
        return in_grace

    def restart_container(self, container_name: str):
        """Reinicia el contenedor fallido"""
        try:
            container = self.client.containers.get(container_name)
            status = container.status
            
            logger.warning(f"Reiniciando {container_name} (status: {status})...")
            container.restart(timeout=10)
            
            self.last_restart_time[container_name] = datetime.now()
            self.failure_counts[container_name] = 0
            
            logger.info(f"{container_name} reiniciado exitosamente")
            logger.info(f"Entrando en grace period de {self.restart_grace_period}s")
            
        except docker.errors.NotFound:
            logger.warning(f"{container_name} no existe - removiendo de monitoreo")
            self.ignored_nodes.add(container_name)
        except Exception as e:
            logger.error(f"Error reiniciando {container_name}: {e}")

    def check_nodes(self):
        active_nodes = self.monitored_nodes - self.ignored_nodes
        
        if not active_nodes:
            logger.warning("No hay nodos activos para monitorear")
            return
        
        for name in active_nodes:
            if self.is_in_grace_period(name):
                if self.check_container_health(name):
                    logger.info(f"{name} saludable durante grace - saliendo de grace")
                    self.failure_counts[name] = 0
                    del self.last_restart_time[name]
                else:
                    logger.info(f"{name} no saludable en grace (tolerado)")
                continue
            
            if self.check_container_health(name):
                if self.failure_counts[name] > 0:
                    logger.info(f"{name} recuperado - reseteando contador")
                self.failure_counts[name] = 0
            else:
                self.failure_counts[name] += 1
                logger.warning(f"{name} fallo {self.failure_counts[name]}/{self.max_failures}")
                
                if self.failure_counts[name] >= self.max_failures:
                    logger.error(f"{name} alcanzó {self.max_failures} fallos - reiniciando")
                    self.restart_container(name)

    def run(self):
        logger.info("Watchdog iniciado (modo Docker nativo)")
        logger.info(f"Config: interval={self.check_interval}s, max_failures={self.max_failures}")
        time.sleep(self.startup_delay)
        
        cycle_count = 0
        while True:
            cycle_count += 1
            
            has_grace_nodes = any(
                self.is_in_grace_period(name) 
                for name in self.monitored_nodes 
                if name not in self.ignored_nodes
            )
            
            interval = self.restart_check_interval if has_grace_nodes else self.check_interval
            logger.debug(f"Ciclo {cycle_count}")
            
            self.check_nodes()
            
            if cycle_count % 10 == 0 and self.ignored_nodes:
                logger.info(f"Nodos ignorados: {', '.join(self.ignored_nodes)}")
            
            time.sleep(interval)


if __name__ == "__main__":
    NodeWatchdog().run()