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
    def __init__(self, kill_interval: int = 1, excluded_containers: Set[str] = None,
                 min_kills: int = 1, max_kills: int = 3, max_total_kills: int = None):
        """
        Args:
            kill_interval: Segundos entre cada ronda de caídas
            excluded_containers: Contenedores que NO deben caerse
            min_kills: Mínimo de contenedores a matar por ronda
            max_kills: Máximo de contenedores a matar por ronda
            max_total_kills: Máximo total de kills antes de detenerse (None = infinito)
        """
        self.client = docker.from_env()
        self.kill_interval = kill_interval
        self.excluded_containers = excluded_containers or set()
        self.min_kills = min_kills
        self.max_kills = max_kills
        self.max_total_kills = max_total_kills
        self.running = True
        self.total_kills = 0
        
    def get_killable_containers(self):
        """Obtiene contenedores que pueden ser detenidos"""
        all_containers = self.client.containers.list(filters={"status": "running"})
        killable = []
        
        for container in all_containers:
            if container.name in self.excluded_containers:
                continue
            
            if any(keyword in container.name.lower() for keyword in [
                # 'groupby_top_customers',
                # 'top_consumers_aggregator',
                # 'groupby_semester_1',
                # 'groupby_best_selling_2024',
                # 'filter_year',
                'filter_year_1','filter_year_2','filter_year_3',
                'filter_amount_1','filter_amount_2','filter_amount_3',
                'filter_hour_1','filter_hour_2','filter_hour_3',
                'dedup_q1',
                # 'groupby', 'join_node', 'aggregator'
                # 'join_node'
            ]):
                killable.append(container)
        
        return killable
    
    def kill_round(self):
        """Mata entre min_kills y max_kills contenedores al azar"""
        killable = self.get_killable_containers()
        
        if not killable:
            logger.warning("No hay contenedores disponibles para matar")
            return 0
        
        # Decidir cuántos matar (aleatorio entre min y max)
        # Pero no más de los disponibles
        num_to_kill = random.randint(self.min_kills, 
                                      min(self.max_kills, len(killable)))
        
        # Si hay límite máximo, no excederlo
        if self.max_total_kills:
            remaining = self.max_total_kills - self.total_kills
            if remaining <= 0:
                logger.info("Límite de kills alcanzado")
                return 0
            num_to_kill = min(num_to_kill, remaining)
        
        # Seleccionar víctimas al azar
        victims = random.sample(killable, num_to_kill)
        
        killed_count = 0
        logger.info(f"Seleccionando {num_to_kill} contenedor(es) para matar...")
        
        for victim in victims:
            try:
                logger.info(f"Matando: {victim.name}")
                victim.kill()
                killed_count += 1
                self.total_kills += 1
            except Exception as e:
                logger.error(f"Error matando {victim.name}: {e}")
        
        if self.max_total_kills:
            logger.info(f"Ronda completada: {killed_count}/{num_to_kill} contenedores caídos (Total: {self.total_kills}/{self.max_total_kills})")
        else:
            logger.info(f"Ronda completada: {killed_count}/{num_to_kill} contenedores caídos (Total: {self.total_kills})")
        
        return killed_count
    
    def chaos_loop(self):
        """Loop principal de caos"""
        logger.info("Chaos Monkey iniciado")
        logger.info(f"Intervalo entre rondas: {self.kill_interval}s")
        logger.info(f"Kills por ronda: {self.min_kills}-{self.max_kills}")
        if self.max_total_kills:
            logger.info(f"Límite total de kills: {self.max_total_kills}")
        logger.info(f"Contenedores protegidos: {self.excluded_containers}")
        
        logger.info("Esperando 30 segundos para que el sistema se estabilice...")
        time.sleep(100)
        
        logger.info("Comenzando el caos...")
        
        while self.running:
            try:
                # Verificar si alcanzamos el límite
                if self.max_total_kills and self.total_kills >= self.max_total_kills:
                    logger.info(f"Límite de {self.max_total_kills} kills alcanzado. Deteniendo Chaos Monkey.")
                    break
                
                self.kill_round()
                
                if self.total_kills % 10 == 0 and self.total_kills > 0:
                    logger.info(f"Estadística: {self.total_kills} contenedores caídos en total")
                
                # Verificar nuevamente después de la ronda
                if self.max_total_kills and self.total_kills >= self.max_total_kills:
                    logger.info(f"Límite de {self.max_total_kills} kills alcanzado. Deteniendo Chaos Monkey.")
                    break
                
                time.sleep(self.kill_interval)
                
            except KeyboardInterrupt:
                logger.info("Chaos Monkey detenido por usuario")
                break
            except Exception as e:
                logger.error(f"Error en chaos loop: {e}")
                time.sleep(self.kill_interval)

        logger.info(f"Total final de contenedores caídos: {self.total_kills}")

    def stop(self):
        """Detiene el chaos monkey"""
        self.running = False

def signal_handler(sig, frame):
    logger.info("Señal de interrupción recibida")
    sys.exit(0)

def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    protected = {
        "client_1",
        "client_2",
        "client_3",
        "gateway",
        "rabbitmq",
        "watchdog",
    }
    
    kill_interval = int(os.getenv("KILL_INTERVAL", "1"))
    min_kills = int(os.getenv("MIN_KILLS", "1"))
    max_kills = int(os.getenv("MAX_KILLS", "9"))
    max_total_kills = int(os.getenv("MAX_TOTAL_KILLS", "200"))
    
    chaos = ChaosMonkey(
        kill_interval=kill_interval,
        excluded_containers=protected,
        min_kills=min_kills,
        max_kills=max_kills,
        max_total_kills=max_total_kills
    )
    
    try:
        chaos.chaos_loop()
    except KeyboardInterrupt:
        chaos.stop()
        logger.info("Chaos Monkey finalizado")

if __name__ == "__main__":
    main()