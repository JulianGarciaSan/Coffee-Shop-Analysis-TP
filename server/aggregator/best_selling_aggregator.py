import logging
import os
import sys
from typing import Dict, Tuple
from collections import defaultdict
from rabbitmq.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
from dtos.dto import TransactionItemBatchDTO, BatchType
from common.graceful_shutdown import GracefulShutdown 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ItemMetrics:    
    def __init__(self, item_id: str):
        self.item_id = item_id
        self.sellings_qty = 0
        self.profit_sum = 0.0
    
    def add_transaction(self, quantity: int, subtotal: float):
        self.sellings_qty += quantity
        self.profit_sum += subtotal
    
    def merge(self, other: 'ItemMetrics'):
        self.sellings_qty += other.sellings_qty
        self.profit_sum += other.profit_sum


class BestSellingAggregatorNode:
    def __init__(self):
        self.shutdown = GracefulShutdown()
        self.shutdown.register_callback(self._on_shutdown_signal)
        
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.mode = os.getenv('AGGREGATOR_MODE', 'intermediate')
        
        logger.info(f"Inicializando BestSellingAggregatorNode con modo: {self.mode}")
        
        self.month_item_metrics: Dict[str, Dict[str, ItemMetrics]] = defaultdict(
            lambda: defaultdict(lambda: ItemMetrics(""))
        )
        
        self.month_item_selling: Dict[str, Dict[str, ItemMetrics]] = defaultdict(
            lambda: defaultdict(lambda: ItemMetrics(""))
        )
        self.month_item_profit: Dict[str, Dict[str, ItemMetrics]] = defaultdict(
            lambda: defaultdict(lambda: ItemMetrics(""))
        )
        
        self.input_middleware = None
        self.output_middleware = None
        
        if self.mode == 'intermediate':
            self.year = os.getenv('AGGREGATOR_YEAR', '2024')
            self.total_groupby_nodes = int(os.getenv('TOTAL_INTERMEDIATE_AGGREGATORS', '2'))
            self.eof_count = 0
            logger.info(f"[INTERMEDIATE] Aggregator intermedio para año {self.year}")
            logger.info(f"  Esperando datos de {self.total_groupby_nodes} nodos GroupBy")
            
        elif self.mode == 'final':
            self.total_intermediate_nodes = int(os.getenv('TOTAL_INTERMEDIATE_AGGREGATORS', '2'))
            self.eof_count = 0
            logger.info(f"[FINAL] Aggregator final global")
            logger.info(f"  Esperando datos de {self.total_intermediate_nodes} aggregators intermedios")
        else:
            logger.error(f"Modo desconocido: {self.mode}. Modos válidos: 'intermediate', 'final'")
            raise ValueError(f"Modo desconocido: {self.mode}")
        
        try:
            self._setup_input_middleware()
            self._setup_output_middleware()
            
            if hasattr(self.input_middleware, 'shutdown'):
                self.input_middleware.shutdown = self.shutdown
            if hasattr(self.output_middleware, 'shutdown'):
                self.output_middleware.shutdown = self.shutdown
            
        except Exception as e:
            logger.error(f"Error durante la configuración de middlewares: {e}")
            raise
    def _on_shutdown_signal(self):
        logger.info("Señal de shutdown recibida en BestSellingAggregator")
        if self.input_middleware:
            self.input_middleware.stop_consuming()
                
    def _setup_input_middleware(self):
        if self.mode == 'intermediate':
            input_exchange = os.getenv('INPUT_EXCHANGE', 'best_selling.exchange')
            queue_name = f'best_selling_intermediate_{self.year}'
            
            self.input_middleware = MessageMiddlewareQueue(
                host=self.rabbitmq_host,
                queue_name=queue_name,
                exchange_name=input_exchange,
                routing_keys=[self.year] 
            )
            logger.info(f"  Input Exchange: {input_exchange}")
            logger.info(f"  Queue: {queue_name} (round-robin con otros nodos)")
            logger.info(f"  Topic: {self.year}")
            
        elif self.mode == 'final':
            input_exchange = os.getenv('INPUT_EXCHANGE', 'best_selling_to_final.exchange')
            input_queue = os.getenv('INPUT_QUEUE', 'best_selling_final')
            
            self.input_middleware = MessageMiddlewareQueue(
                host=self.rabbitmq_host,
                queue_name=input_queue,
                exchange_name=input_exchange,
                routing_keys=['top_selling.data','top_profit.data']
            )
            logger.info(f"  Input Exchange: {input_exchange}")
            logger.info(f"  Input Queue: {input_queue}")
            logger.info(f"  Routing keys: top_selling.*, top_profit.*")
    
    def _setup_output_middleware(self):
        if self.mode == 'intermediate':
            output_exchange = os.getenv('OUTPUT_EXCHANGE', 'best_selling_to_final.exchange')
            route_keys = [
                'top_selling.data', 
                'top_profit.data', 
            ]
            self.output_middleware = MessageMiddlewareExchange(
                host=self.rabbitmq_host,
                exchange_name=output_exchange,
                route_keys=route_keys
            )
            logger.info(f"  Output Exchange: {output_exchange}")
            
        elif self.mode == 'final':
            output_exchange = os.getenv('OUTPUT_EXCHANGE', 'join.exchange')
            route_keys = [
                'q2_best_selling.data',
                'q2_most_profit.data',
            ]
            self.output_middleware = MessageMiddlewareExchange(
                host=self.rabbitmq_host,
                exchange_name=output_exchange,
                route_keys=route_keys
            )
            logger.info(f"  Output Exchange: {output_exchange}")
    
    def process_csv_line(self, csv_line: str, routing_key: str = None):
        try:
            parts = csv_line.split(',')
            if len(parts) < 3 or parts[0] == 'created_at':
                return
            
            year_month = parts[0]
            item_id = parts[1]
            
            if self.mode == 'intermediate':
                if len(parts) < 4:
                    return
                sellings_qty = int(parts[2])
                profit_sum = float(parts[3])
                
                if item_id not in self.month_item_metrics[year_month]:
                    self.month_item_metrics[year_month][item_id] = ItemMetrics(item_id)
                
                self.month_item_metrics[year_month][item_id].sellings_qty += sellings_qty
                self.month_item_metrics[year_month][item_id].profit_sum += profit_sum
                
            elif self.mode == 'final':
                if routing_key and 'top_selling' in routing_key:
                    sellings_qty = int(parts[2])
                    
                    if item_id not in self.month_item_selling[year_month]:
                        self.month_item_selling[year_month][item_id] = ItemMetrics(item_id)
                    
                    self.month_item_selling[year_month][item_id].sellings_qty += sellings_qty
                    
                elif routing_key and 'top_profit' in routing_key:
                    profit_sum = float(parts[2])
                    
                    if item_id not in self.month_item_profit[year_month]:
                        self.month_item_profit[year_month][item_id] = ItemMetrics(item_id)
                    
                    self.month_item_profit[year_month][item_id].profit_sum += profit_sum
            
        except (ValueError, IndexError) as e:
            logger.warning(f"Error procesando línea: {csv_line}, error: {e}")
    
    def calculate_top1_per_month(self) -> Tuple[Dict[str, Tuple[str, int]], Dict[str, Tuple[str, float]]]:
        """Calcula top 1 según el modo"""
        best_selling = {}
        most_profit = {}
        
        if self.mode == 'intermediate':
            for year_month in sorted(self.month_item_metrics.keys()):
                items = self.month_item_metrics[year_month]
                
                if not items:
                    continue
                
                top_selling_item = max(items.values(), key=lambda x: (x.sellings_qty, -int(x.item_id)))
                best_selling[year_month] = (top_selling_item.item_id, top_selling_item.sellings_qty)
                
                top_profit_item = max(items.values(), key=lambda x: (x.profit_sum, -int(x.item_id)))
                most_profit[year_month] = (top_profit_item.item_id, top_profit_item.profit_sum)
                
        elif self.mode == 'final':
            for year_month in sorted(self.month_item_selling.keys()):
                items = self.month_item_selling[year_month]
                
                if items:
                    top_selling_item = max(items.values(), key=lambda x: (x.sellings_qty, -int(x.item_id)))
                    best_selling[year_month] = (top_selling_item.item_id, top_selling_item.sellings_qty)
            
            for year_month in sorted(self.month_item_profit.keys()):
                items = self.month_item_profit[year_month]
                
                if items:
                    top_profit_item = max(items.values(), key=lambda x: (x.profit_sum, -int(x.item_id)))
                    most_profit[year_month] = (top_profit_item.item_id, top_profit_item.profit_sum)
        
        return best_selling, most_profit
    
    def generate_top1_csv(self, top_dict: Dict[str, Tuple[str, float]], metric_name: str) -> str:
        csv_lines = [f"created_at,item_id,{metric_name}"]
        
        for year_month in sorted(top_dict.keys()):
            item_id, value = top_dict[year_month]
            if metric_name == "sellings_qty":
                csv_lines.append(f"{year_month},{item_id},{int(value)}")
            else:
                csv_lines.append(f"{year_month},{item_id},{value:.1f}")
        
        return '\n'.join(csv_lines)
    
    def handle_eof(self, dto: TransactionItemBatchDTO, routing_key: str = None) -> bool:
        try:
            eof_data = dto.data.strip()
            counter = int(eof_data.split(':')[1]) if ':' in eof_data else 1
            
            if self.mode == 'intermediate':
                logger.info(f"EOF recibido con counter={counter}, total={self.total_groupby_nodes}")
                                
                if counter < self.total_groupby_nodes:
                    new_counter = counter + 1
                    eof_dto = TransactionItemBatchDTO(f"EOF:{new_counter}", BatchType.EOF)
                    self.input_middleware.send(eof_dto.to_bytes_fast())
                    logger.info(f"EOF:{new_counter} reenviado a input queue")
                    logger.info("Datos enviados - esperando otros nodos")
                    #return False
                else:
                    logger.info(f"Último nodo intermediate del año {self.year} procesado")
                    eof_dto = TransactionItemBatchDTO("EOF:1", BatchType.EOF)
                    self._send_intermediate_results()
                    self.output_middleware.send(
                        eof_dto.to_bytes_fast(), 
                        routing_key='top_selling.data'
                    )
                    self.output_middleware.send(
                        eof_dto.to_bytes_fast(), 
                        routing_key='top_profit.data'
                    )
                    #return True
                return True
                    
            elif self.mode == 'final':
                if routing_key and 'top_selling' in routing_key:
                    logger.info(f"EOF selling recibido (de año con counter={counter})")
                    self.eof_count_selling = getattr(self, 'eof_count_selling', 0) + 1
                    logger.info(f"EOFs selling recibidos: {self.eof_count_selling}/2")
                        
                elif routing_key and 'top_profit' in routing_key:
                    logger.info(f"EOF profit recibido (de año con counter={counter})")
                    self.eof_count_profit = getattr(self, 'eof_count_profit', 0) + 1
                    logger.info(f"EOFs profit recibidos: {self.eof_count_profit}/2")
                
                total_years = 2
                if (getattr(self, 'eof_count_selling', 0) >= total_years and
                    getattr(self, 'eof_count_profit', 0) >= total_years):
                    logger.info(f"[FINAL] Todos los años terminaron (selling y profit)")
                    self._send_final_results()
                    return True
                    
                return False
            
            return False
            
        except Exception as e:
            logger.error(f"Error manejando EOF: {e}")
            return False
    
    def _send_intermediate_results(self):
        best_selling, most_profit = self.calculate_top1_per_month()
        
        selling_csv = self.generate_top1_csv(best_selling, "sellings_qty")
        selling_dto = TransactionItemBatchDTO(selling_csv, BatchType.RAW_CSV)
        self.output_middleware.send(
            selling_dto.to_bytes_fast(), 
            routing_key='top_selling.data'
        )
        
        profit_csv = self.generate_top1_csv(most_profit, "profit_sum")
        profit_dto = TransactionItemBatchDTO(profit_csv, BatchType.RAW_CSV)
        self.output_middleware.send(
            profit_dto.to_bytes_fast(), 
            routing_key='top_profit.data'
        )
        
        logger.info(f"[INTERMEDIATE] Resultados enviados para año {self.year}")
        logger.info(f"  Top selling: {len(best_selling)} meses")
        logger.info(f"  Most profit: {len(most_profit)} meses")
    
    def _send_final_results(self):
        best_selling, most_profit = self.calculate_top1_per_month()
        
        selling_csv = self.generate_top1_csv(best_selling, "sellings_qty")
        selling_dto = TransactionItemBatchDTO(selling_csv, BatchType.RAW_CSV)
        self.output_middleware.send(
            selling_dto.to_bytes_fast(), 
            routing_key='q2_best_selling.data'
        )
        
        eof_dto = TransactionItemBatchDTO("EOF:1", BatchType.EOF)
        self.output_middleware.send(
            eof_dto.to_bytes_fast(), 
            routing_key='q2_best_selling.data'
        )
        
        profit_csv = self.generate_top1_csv(most_profit, "profit_sum")
        profit_dto = TransactionItemBatchDTO(profit_csv, BatchType.RAW_CSV)
        self.output_middleware.send(
            profit_dto.to_bytes_fast(), 
            routing_key='q2_most_profit.data'
        )
        
        self.output_middleware.send(
            eof_dto.to_bytes_fast(), 
            routing_key='q2_most_profit.data'
        )
        
        logger.info("[FINAL] Resultados finales enviados al JOIN")
        logger.info(f"  Top selling: {len(best_selling)} meses")
        logger.info(f"  Most profit: {len(most_profit)} meses")
        
    
    def process_message(self, message: bytes, routing_key: str = None) -> bool:
        try:
            if self.shutdown.is_shutting_down():
                logger.warning("Shutdown en progreso, ignorando mensaje")
                return True
            
            dto = TransactionItemBatchDTO.from_bytes_fast(message)
            
            if dto.batch_type == BatchType.EOF:
                return self.handle_eof(dto, routing_key)
            
            if dto.batch_type == BatchType.RAW_CSV:
                for line in dto.data.split('\n'):
                    if line.strip():
                        self.process_csv_line(line.strip(), routing_key)
            
            return False
            
        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")
            return False
    
    def on_message_callback(self, ch, method, properties, body):
        try:
            if self.shutdown.is_shutting_down():
                logger.warning("Shutdown solicitado, deteniendo")
                ch.stop_consuming()
                return
            
            routing_key = getattr(method, 'routing_key', None)
            should_stop = self.process_message(body, routing_key)
            if should_stop:
                logger.info("EOF completo - deteniendo consuming")
                ch.stop_consuming()
        except Exception as e:
            logger.error(f"Error en callback: {e}")
    
    def start(self):
        try:
            mode_str = f"Intermedio ({self.year})" if self.mode == 'intermediate' else "Final"
            logger.info(f"Iniciando BestSellingAggregator {mode_str}...")
            
            if self.input_middleware is None:
                logger.error("input_middleware no está inicializado")
                raise RuntimeError("input_middleware no está inicializado")
                
            self.input_middleware.start_consuming(self.on_message_callback)
        except KeyboardInterrupt:
            logger.info("Aggregator detenido manualmente")
        except Exception as e:
            logger.error(f"Error durante el consumo: {e}")
            raise
        finally:
            self._cleanup()
    
    def _cleanup(self):
        try:
            if self.input_middleware:
                self.input_middleware.close()
            if self.output_middleware:
                self.output_middleware.close()
            logger.info("Conexiones cerradas")
        except Exception as e:
            logger.error(f"Error durante cleanup: {e}")


if __name__ == "__main__":
    try:
        aggregator = BestSellingAggregatorNode()
        aggregator.start()
        logger.info("BestSellingAggregator terminado exitosamente")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error fatal: {e}")
        sys.exit(1)