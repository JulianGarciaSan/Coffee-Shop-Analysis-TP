import docker
import time
import random
import logging
import signal
import sys
import os
from typing import Set

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [CHAOS] - %(message)s")
logger = logging.getLogger(__name__)

class ChaosMonkey:
    def __init__(self, kill_interval: int = 1, excluded_containers: Set[str] = None):
        """
        Script que mata contenedores al azar.
        
        Args:
            kill_interval: Segundos entre cada caída de nodo
            excluded_containers: Nombres de contenedores que NO deben caerse
        """
        self.client = docker.from_env()
        self.kill_interval = kill_interval
        self.excluded_containers = excluded_containers or set()
        self.running = True
        self.kill_count = 0
        self.kill_history = []  # Historial de caídas
        
    def get_killable_containers(self):
        """Obtiene contenedores que pueden ser detenidos"""
        all_containers = self.client.containers.list(filters={"status": "running"})
        killable = []
        
        for container in all_containers:
            # Excluir protegidos
            if container.name in self.excluded_containers:
                continue
            
            # Solo incluir nodos del sistema distribuido
            if any(keyword in container.name.lower() for keyword in [
                'filter_year',
                'groupby', 'join_node', 'aggregator'
            ]):
                killable.append(container)
        
        return killable
    
    def kill_random_container(self):
        """Mata un contenedor al azar"""
        killable = self.get_killable_containers()
        
        if not killable:
            logger.warning("No hay contenedores disponibles para matar")
            return False
        
        victim = random.choice(killable)
        
        try:
            logger.info(f"🔥 Matando contenedor: {victim.name}")
            victim.kill()  # Usa kill en lugar de stop para simular crash abrupto
            self.kill_count += 1
            self.kill_history.append({
                'name': victim.name,
                'time': time.time()
            })
            logger.info(f"💀 Contenedor {victim.name} detenido. Total: {self.kill_count}")
            return True
        except Exception as e:
            logger.error(f"❌ Error matando {victim.name}: {e}")
            return False
    
    def print_stats(self):
        """Imprime estadísticas cada 10 kills"""
        if self.kill_count % 10 == 0 and self.kill_count > 0:
            logger.info(f"📊 Estadísticas: {self.kill_count} contenedores caídos")
            
            # Contar por tipo
            from collections import Counter
            types = Counter([h['name'].split('_')[0] + '_' + h['name'].split('_')[1] 
                           for h in self.kill_history])
            for container_type, count in types.most_common():
                logger.info(f"   - {container_type}: {count} caídas")
    
    def chaos_loop(self):
        """Loop principal de caos"""
        logger.info("🐵 Chaos Monkey iniciado")
        logger.info(f"⏱️  Intervalo de caídas: {self.kill_interval}s")
        logger.info(f"🛡️  Contenedores protegidos: {self.excluded_containers}")
        
        # Esperar a que el sistema esté estable
        logger.info("⏳ Esperando 30 segundos para que el sistema se estabilice...")
        time.sleep(30)
        
        logger.info("🎯 Comenzando el caos...")
        
        while self.running:
            try:
                self.kill_random_container()
                self.print_stats()
                time.sleep(self.kill_interval)
                
            except KeyboardInterrupt:
                logger.info("⛔ Chaos Monkey detenido por usuario")
                break
            except Exception as e:
                logger.error(f"❌ Error en chaos loop: {e}")
                time.sleep(self.kill_interval)
        
        logger.info(f"📈 Total de contenedores caídos: {self.kill_count}")
    
    def stop(self):
        """Detiene el chaos monkey"""
        self.running = False

def signal_handler(sig, frame):
    logger.info("⛔ Señal de interrupción recibida")
    sys.exit(0)

def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Contenedores que NO deben caerse
    protected = {
        "client_1",
        "gateway",
        "rabbitmq",
        "watchdog",
    }
    
    # Leer intervalo de variable de entorno
    kill_interval = int(os.getenv("KILL_INTERVAL", "1"))
    
    chaos = ChaosMonkey(kill_interval=kill_interval, excluded_containers=protected)
    
    try:
        chaos.chaos_loop()
    except KeyboardInterrupt:
        chaos.stop()
        logger.info("🏁 Chaos Monkey finalizado")

if __name__ == "__main__":
    main()