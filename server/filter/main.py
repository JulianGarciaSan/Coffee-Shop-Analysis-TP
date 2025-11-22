from datetime import datetime
import logging
import os
import sys
import time
import threading
from typing import Optional
from rabbitmq.middleware import MessageMiddlewareQueue
from logger_monitor import LoggerMonitor
from healthchecker.healthchecker import HealthChecker
from strategies import FilterStrategyFactory
from configurators import NodeConfiguratorFactory
from dtos.dto import TransactionBatchDTO, TransactionItemBatchDTO, BatchType, FileType
from common.graceful_shutdown import GracefulShutdown  
from consensus_node import ConsensusNode

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FilterNode:
    def __init__(self):
        self.shutdown = GracefulShutdown()
        self.shutdown.register_callback(self._on_shutdown_signal)
        
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.input_queue = os.getenv('INPUT_QUEUE', None)
        self.output_q1 = os.getenv('OUTPUT_Q1', None)
        self.output_q2 = os.getenv('OUTPUT_Q2', None)
        self.output_q3 = os.getenv('OUTPUT_Q3', None)
        self.output_q4 = os.getenv('OUTPUT_Q4', None)
        self.filter_mode = os.getenv('FILTER_MODE', 'year')
        self.input_exchange = os.getenv('INPUT_EXCHANGE', None)

        
        total_env_var = f'TOTAL_{self.filter_mode.upper()}_FILTERS'
        self.total_filters = int(os.getenv(total_env_var, '1'))
        
        self.node_id = os.getenv('NODE_ID', None)
        all_node_ids_str = os.getenv('ALL_NODE_IDS', self.node_id)
        all_node_ids = all_node_ids_str.split(',') if all_node_ids_str else [self.node_id]
        
        
        logger.info(f"FilterNode inicializado:")
        logger.info(f"  Modo: {self.filter_mode}")
        logger.info(f"  Queue entrada: {self.input_queue}")
        logger.info(f"  Total filtros {self.filter_mode}: {self.total_filters}")
        
        self.filter_strategy = self._create_filter_strategy()
        
        self.logger = LoggerMonitor('/app/logs.txt')
        self.client_logger = LoggerMonitor('/app/client_logs.txt')
        self.eof_logger = LoggerMonitor('/app/eof_logs.txt')
        self.is_first_message = True
        
        self.node_configurator = NodeConfiguratorFactory.create_configurator(
            self.filter_mode,
            self.rabbitmq_host
        , self.logger, self.client_logger, self.eof_logger
        )
                
        if self.filter_mode == 'year':
            self.input_middleware = self.node_configurator.create_input_middleware(
                self.input_exchange, self.input_queue
            )
        else: 
            self.input_middleware = self.node_configurator.create_input_middleware(
                self.input_queue, ""
            )
        
        if hasattr(self.input_middleware, 'shutdown'):
            self.input_middleware.shutdown = self.shutdown
        
        self.middlewares = self.node_configurator.create_output_middlewares(
            self.output_q1,
            self.output_q3,
            self.output_q4,
            self.output_q2
        )       
         
        for name, middleware in self.middlewares.items():
            if middleware and hasattr(middleware, 'shutdown'):
                middleware.shutdown = self.shutdown
                
        health_port = int(os.getenv('HEALTH_PORT', '9999'))
        self.health_server = HealthChecker(port=health_port)
        self.health_server.start()
        


    def _on_shutdown_signal(self):
        logger.info("FilterNode: Señal de shutdown recibida, deteniendo consumo...")
        try:
            if self.input_middleware:
                self.input_middleware.stop_consuming()
        except Exception as e:
            logger.error(f"Error deteniendo consumo: {e}")
            
    def _create_filter_strategy(self):
        try:
            config = {}
            
            if self.filter_mode == 'year':
                config['filter_years'] = os.getenv('FILTER_YEARS', '2024,2025')
            elif self.filter_mode == 'hour':
                config['filter_hours'] = os.getenv('FILTER_HOURS', '06:00-23:00')
            elif self.filter_mode == 'amount':
                config['min_amount'] = float(os.getenv('MIN_AMOUNT', '75'))
            
            return FilterStrategyFactory.create_strategy(self.filter_mode, **config)
            
        except Exception as e:
            logger.error(f"Error creando estrategia de filtro: {e}")
            raise

    def _extract_client_id(self, data: str) -> str:
        if data.startswith("EOF:"):
            parts = data.split(':', 2)
            if len(parts) >= 2:
                return parts[1]
        elif ':' in data:
            return data.split(':', 1)[0]
        
        return "default"

    def process_message(self, body: bytes, routing_key: str = None, client_id: int = None,message_id:int = None):
        if self.shutdown.is_shutting_down():
            logger.warning("Shutdown en progreso, ignorando mensaje")
        
        try:
            result = self.node_configurator.process_message(body, routing_key, client_id, message_id)
            
            if len(result) == 5:
                should_stop, batch_type, dto, is_eof, is_dup = result
            else:
                should_stop, batch_type, dto, is_eof = result
                is_dup = False 
                
            # if is_eof:
            #     return self._handle_eof_message(dto, batch_type, client_id)
            
            if is_eof:
                self.logger.write_with_timestamp(f"END:{client_id}")
                return False
            
            if is_dup:
                logger.info(f"Mensaje duplicado detectado para client_id {client_id}, message_id {message_id}. Ignorando procesamiento.")
                return False

            if should_stop:
                return True
                        
            decoded_data = body.decode('utf-8').strip()
            
            if hasattr(self.filter_strategy, 'set_dto_helper'):
                self.filter_strategy.set_dto_helper(dto)
            
            filtered_csv = self.filter_strategy.filter_csv_batch(decoded_data)
            
            if not filtered_csv.strip():
                return False
            
            if self.shutdown.is_shutting_down():
                logger.warning("Shutdown en progreso, no se enviarán datos")
                return True
                        
            processed_data = self.node_configurator.process_filtered_data(filtered_csv)
            self.logger.write_with_timestamp(f"Informo que Filtre el mensaje")
            self.node_configurator.send_data(processed_data, self.middlewares, batch_type, client_id=client_id,message_id=message_id)
            self.logger.write_with_timestamp(f"Informo que encole el mensaje")
            # time.sleep(30)
            self.logger.write_with_timestamp(f"Termine la iteracion")
            return False

        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")
            return False
        
    def on_message_callback(self, ch, method, properties, body):
        try:
            #logging.info("Mensaje recibido en FilterNode")
            if self.shutdown.is_shutting_down():
                logger.warning("Shutdown solicitado, deteniendo consumo")
                ch.stop_consuming()
                return
            
            client_id = None
            message_id = None
            if properties and properties.headers:
                client_id = properties.headers.get('client_id')
                message_id = properties.headers.get('message_id')
                
                
            routing_key = method.routing_key if hasattr(method, 'routing_key') else None
            
            if self.is_first_message:
                self.is_first_message = False
                last_log = self.logger._get_last_line()
                # logger.info(f"Esto es la ultima linea del logger: {last_log}")
                
                if self.analize_log(last_log, client_id, message_id):
                    logger.info("Mensaje ya procesado, haciendo ACK y continuando")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    self.logger.write_with_timestamp(f"Termine la iteracion")
                    return

            self.client_logger.write(f"{client_id};{message_id}")
            logging.info("Mensaje recibido en FilterNode")
            should_stop = self.process_message(body, routing_key, client_id,message_id)
            #time.sleep(30)
            ch.basic_ack(delivery_tag=method.delivery_tag)

            
            if should_stop:
                logger.info("EOF procesado - deteniendo consuming")
                ch.stop_consuming()
        except Exception as e:
            logger.error(f"Error en callback: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


    def pre_analyze_eof_log(self):
        logger.info(f"Analizando EOF log en pre_analyze_eof_log")
        last_line = self.eof_logger._get_last_line()
        logger.info(f"Ultima linea del eof log: {last_line}")
        
        if not last_line or ':' not in last_line:
            logger.warning("No hay logs EOF previos válidos")
            return True
        
        parts = last_line.split(':', 2)
        eof_status = parts[0] if len(parts) > 0 else ""
        client_id_eof_log = parts[1] if len(parts) > 1 else ""
        batch_type_eof_log = parts[2] if len(parts) > 2 else ""
        
        if "EOF" in eof_status:
            logger.info(f"Procesando EOF pendiente para client_id {client_id_eof_log}")
            self.node_configurator._on_all_acks_received(client_id_eof_log, batch_type_eof_log)
            return False

        if "BEF" in eof_status:
            logger.info(f"Reenviando EOF pendiente para client_id {client_id_eof_log}")
            self.node_configurator.process_message(TransactionBatchDTO("EOF:1", BatchType.EOF).to_bytes_fast(), None, client_id_eof_log)
            return False

        return True
    
    def analize_log(self, log, client_id, message_id):
        logger.info(f"Analizando log en analize_log: {log}")
        last_line_client = self.client_logger._get_last_line()
        logger.info(f"Ultima linea del client log: {last_line_client}")
        last_line_eof = self.eof_logger._get_last_line()
        logger.info(f"Ultima linea del eof log: {last_line_eof}")
        
        client_id = str(client_id) if client_id is not None else ""
        message_id = str(message_id) if message_id is not None else ""
        
        # Parsear client log (si existe)
        client_id_client_log = None
        message_id_client_log = None
        if last_line_client and ';' in last_line_client:
            try:
                client_id_client_log, message_id_client_log = last_line_client.split(';')
            except ValueError:
                logger.warning(f"Error parseando client log: {last_line_client}")
        
        # Parsear EOF log (si existe)
        eof_eof_log = None
        client_id_eof_log = None
        batch_type_eof_log = None
        if last_line_eof and ':' in last_line_eof:
            try:
                parts = last_line_eof.split(':')
                if len(parts) >= 3:
                    eof_eof_log, client_id_eof_log, batch_type_eof_log = parts[0], parts[1], parts[2]
            except ValueError:
                logger.warning(f"Error parseando eof log: {last_line_eof}")
        
        # Ahora evaluar los casos con los datos parseados (o None si fallaron)
        
        if "END" in log:
            if eof_eof_log == "END":
                if client_id == client_id_client_log and message_id == message_id_client_log:
                    return True
                return False
            
            if eof_eof_log == "EOF":
                if client_id_eof_log and batch_type_eof_log:
                    self.node_configurator._on_all_acks_received(client_id_eof_log, batch_type_eof_log)
                
                # Esto es del mensaje de los logs client
                if client_id == client_id_client_log and message_id == message_id_client_log:
                    return True
                
                return False
            
            if eof_eof_log == "BEF":
                if client_id_eof_log:
                    self.node_configurator.process_message(
                        TransactionBatchDTO("EOF:1", BatchType.EOF).to_bytes_fast(), 
                        None, 
                        client_id_eof_log
                    )
                return False
            
        if "EOF" in log:
            if eof_eof_log == "END":
                # De alguna manera llegaron todos los acks, antes de que envíe el ack del Primero EOF recibido
                # Solo falta mandar el ACK del primero EOF recibido
                if client_id_client_log == client_id_eof_log == client_id and message_id_client_log == message_id:
                    return True
                return False
            
            if eof_eof_log == "EOF":
                if client_id_eof_log and batch_type_eof_log:
                    self.node_configurator._on_all_acks_received(client_id_eof_log, batch_type_eof_log)
                
                # Solo falta mandar el ACK del primero EOF recibido
                if client_id_client_log == client_id_eof_log == client_id and message_id_client_log == message_id:
                    return True
                return False
        
            return False

        if "Informo que encole el mensaje" in log:
            if client_id_client_log == client_id and message_id_client_log == message_id:
                logger.info(f"FilterNode: Mensaje encolado para client_id {client_id}")
                return True
            logger.info(f"FilterNode: Mensaje no corresponde a client_id {client_id}, client_id log: {client_id_client_log}, message_id log: {message_id_client_log}, message_id: {message_id}")

        if "Informo que Filtre" in log:
            logger.info(f"FilterNode: Reintento filtrar y enviar el mensaje")
            return False
        
        return False

    def start(self):
        try:
            logger.info("Iniciando consumo de mensajes...")
            self.is_first_message = self.pre_analyze_eof_log()
            self.input_middleware.start_consuming(self.on_message_callback)
        except KeyboardInterrupt:
            logger.info("Filtro detenido manualmente")
        except Exception as e:
            logger.error(f"Error durante el consumo: {e}")
            raise
        finally:
            self._cleanup()
                   

    def _cleanup(self):
        """Limpieza ordenada de recursos"""
        logger.info("Iniciando cleanup del FilterNode...")
        
        try:
            if hasattr(self.node_configurator, 'close'):
                self.node_configurator.close()
            # Cerrar middleware de entrada
            if self.input_middleware:
                self.input_middleware.close()
                logger.info("Middleware de entrada cerrado")
            
            # Cerrar todos los middlewares de salida
            for name, middleware in self.middlewares.items():
                if middleware:
                    try:
                        middleware.close()
                        logger.info(f"Middleware '{name}' cerrado")
                    except Exception as e:
                        logger.error(f"Error cerrando middleware '{name}': {e}")
             
            if self.health_server:
                self.health_server.stop()
                self.health_server.join(timeout=5.0)
                logger.info("HealthChecker detenido")
                   
        except Exception as e:
            logger.error(f"Error durante cleanup: {e}")

        logger.info("Cleanup completado")


if __name__ == "__main__":
    try:
        filter_node = FilterNode()
        filter_node.start()
        logger.info("FilterNode terminado exitosamente")
        sys.exit(0)     
    except Exception as e:
        logger.error(f"Error fatal: {e}")
        sys.exit(1)