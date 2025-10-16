# best_selling_aggregator.py
import logging
import os
import sys
from typing import Dict, Tuple
from collections import defaultdict
from rabbitmq.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
from dtos.dto import TransactionItemBatchDTO, BatchType
from common.graceful_shutdown import GracefulShutdown
from client_routing.client_routing import ClientRouter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ItemMetrics:
    def __init__(self, item_id: str):
        self.item_id = item_id
        self.sellings_qty = 0
        self.profit_sum = 0.0


class BestSellingAggregatorNode:
    def __init__(self):
        self.shutdown = GracefulShutdown()
        self.shutdown.register_callback(self._on_shutdown_signal)
        
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.total_years = int(os.getenv('TOTAL_YEARS', '2'))
        self.total_groupby_nodes_per_year = int(os.getenv('TOTAL_GROUPBY_NODES', '4'))
        
        self.expected_sources = self.total_years * self.total_groupby_nodes_per_year
        
        total_join_nodes = int(os.getenv('TOTAL_JOIN_NODES', '1'))
        self.client_router = ClientRouter(total_join_nodes, node_prefix="join_node")
        
        self.month_selling_candidates_by_client: Dict[str, Dict[str, list]] = defaultdict(
            lambda: defaultdict(list)
        )
        self.month_profit_candidates_by_client: Dict[str, Dict[str, list]] = defaultdict(
            lambda: defaultdict(list)
        )
        
        self.eof_selling_count_by_client: Dict[str, int] = defaultdict(int)
        self.eof_profit_count_by_client: Dict[str, int] = defaultdict(int)
        
        logger.info(f"BestSellingAggregatorFinal inicializado")
        logger.info(f"  Total años: {self.total_years}")
        logger.info(f"  Nodos GroupBy por año: {self.total_groupby_nodes_per_year}")
        logger.info(f"  Esperando datos de {self.expected_sources} fuentes")
        logger.info(f"  Total nodos join: {total_join_nodes}")
        
        self._setup_middleware()
    
    def _setup_middleware(self):
        input_exchange = os.getenv('INPUT_EXCHANGE', 'best_selling_to_final.exchange')
        input_queue = os.getenv('INPUT_QUEUE', 'best_selling_final')
        
        self.input_middleware = MessageMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=input_queue,
            exchange_name=input_exchange,
            routing_keys=['top_selling.data', 'top_profit.data']
        )
        
        self.input_middleware.shutdown = self.shutdown
        
        logger.info(f"  Input Exchange: {input_exchange}")
        logger.info(f"  Input Queue: {input_queue}")
        
        output_exchange = os.getenv('OUTPUT_EXCHANGE', 'join.exchange')
        
        route_keys = []
        route_keys.extend(self.client_router.get_all_routing_keys('q2_best_selling.data'))
        route_keys.extend(self.client_router.get_all_routing_keys('q2_most_profit.data'))
        
        self.output_middleware = MessageMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name=output_exchange,
            route_keys=route_keys
        )
        
        self.output_middleware.shutdown = self.shutdown
        
        logger.info(f"  Output Exchange: {output_exchange}")
        
        if hasattr(self.input_middleware, 'shutdown'):
            self.input_middleware.shutdown = self.shutdown
        if hasattr(self.output_middleware, 'shutdown'):
            self.output_middleware.shutdown = self.shutdown
    
    def _on_shutdown_signal(self):
        logger.info("Señal de shutdown recibida")
        if self.input_middleware:
            self.input_middleware.stop_consuming()
    
    def process_csv_line(self, csv_line: str, routing_key: str, client_id: str):
        """Procesa línea de top 1 candidato"""
        try:
            parts = csv_line.split(',')
            if len(parts) < 3 or parts[0] == 'created_at':
                return
            
            year_month = parts[0]
            item_id = parts[1]
            
            if 'top_selling' in routing_key:
                sellings_qty = int(parts[2])
                
                self.month_selling_candidates_by_client[client_id][year_month].append({
                    'item_id': item_id,
                    'sellings_qty': sellings_qty
                })
                
                logger.debug(f"Candidato selling: {year_month}, item {item_id}, qty {sellings_qty}")
                
            elif 'top_profit' in routing_key:
                profit_sum = float(parts[2])
                
                self.month_profit_candidates_by_client[client_id][year_month].append({
                    'item_id': item_id,
                    'profit_sum': profit_sum
                })
                
                logger.debug(f"Candidato profit: {year_month}, item {item_id}, profit ${profit_sum:.2f}")
            
        except (ValueError, IndexError) as e:
            logger.warning(f"Error procesando línea: {csv_line}, error: {e}")
    
    def calculate_global_top1(self, client_id: str) -> Tuple[Dict, Dict]:
        """
        Calcula el top 1 GLOBAL por mes considerando todos los candidatos
        de todos los nodos GroupBy de todos los años
        """
        best_selling = {}
        most_profit = {}
        
        selling_candidates = self.month_selling_candidates_by_client.get(client_id, {})
        for year_month, candidates in selling_candidates.items():
            if not candidates:
                continue
            
            logger.info(f"===== CANDIDATOS SELLING para {year_month}, cliente {client_id} =====")
            for i, cand in enumerate(candidates):
                logger.info(f"  Candidato {i}: item_id='{cand['item_id']}', sellings_qty={cand['sellings_qty']}")
            
            top = max(candidates, 
                    key=lambda x: (x['sellings_qty'], -int(x['item_id']) if x['item_id'].isdigit() else 0))
            
            best_selling[year_month] = (top['item_id'], top['sellings_qty'])
            
            logger.info(f"  >>> GANADOR: item_id='{top['item_id']}', sellings_qty={top['sellings_qty']}")
            logger.info(f"=" * 70)
        
        profit_candidates = self.month_profit_candidates_by_client.get(client_id, {})
        for year_month, candidates in profit_candidates.items():
            if not candidates:
                continue
            
            logger.info(f"===== CANDIDATOS PROFIT para {year_month}, cliente {client_id} =====")
            for i, cand in enumerate(candidates):
                logger.info(f"  Candidato {i}: item_id='{cand['item_id']}', profit_sum={cand['profit_sum']}")
            
            top = max(candidates,
                    key=lambda x: (x['profit_sum'], -int(x['item_id']) if x['item_id'].isdigit() else 0))
            
            most_profit[year_month] = (top['item_id'], top['profit_sum'])
            
            logger.info(f"  >>> GANADOR: item_id='{top['item_id']}', profit_sum={top['profit_sum']}")
            logger.info(f"=" * 70)
        
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
    
    def handle_eof(self, routing_key: str, client_id: str) -> bool:
        try:
            logger.info(f"EOF recibido para cliente {client_id}, routing_key={routing_key}")
            
            if 'top_selling' in routing_key:
                self.eof_selling_count_by_client[client_id] += 1
                logger.info(f"EOF selling #{self.eof_selling_count_by_client[client_id]}/{self.expected_sources} para cliente {client_id}")
                
            elif 'top_profit' in routing_key:
                self.eof_profit_count_by_client[client_id] += 1
                logger.info(f"EOF profit #{self.eof_profit_count_by_client[client_id]}/{self.expected_sources} para cliente {client_id}")
            
            selling_count = self.eof_selling_count_by_client[client_id]
            profit_count = self.eof_profit_count_by_client[client_id]
            
            if selling_count == self.expected_sources and profit_count == self.expected_sources:
                logger.info(f"Todos los EOFs recibidos para cliente {client_id}, calculando top 1 global")
                self._send_final_results(client_id)
                
                # Cleanup
                del self.eof_selling_count_by_client[client_id]
                del self.eof_profit_count_by_client[client_id]
                del self.month_selling_candidates_by_client[client_id]
                del self.month_profit_candidates_by_client[client_id]
            
            return False
            
        except Exception as e:
            logger.error(f"Error manejando EOF: {e}")
            return False
    
    def _send_final_results(self, client_id: str):
        """Calcula top 1 global y envía al JOIN"""
        best_selling, most_profit = self.calculate_global_top1(client_id)
        
        headers = {'client_id': client_id}
        
        selling_routing_key = self.client_router.get_routing_key(client_id, 'q2_best_selling.data')
        profit_routing_key = self.client_router.get_routing_key(client_id, 'q2_most_profit.data')
        
        selling_csv = self.generate_top1_csv(best_selling, "sellings_qty")
        selling_dto = TransactionItemBatchDTO(selling_csv, BatchType.RAW_CSV)
        
        logger.info(f"Enviando best selling: {len(selling_csv)} bytes → {selling_routing_key}")
        self.output_middleware.send(
            selling_dto.to_bytes_fast(),
            routing_key=selling_routing_key,
            headers=headers
        )
        selling_eof = TransactionItemBatchDTO("EOF:1", BatchType.EOF)
        self.output_middleware.send(
            selling_eof.to_bytes_fast(),
            routing_key=selling_routing_key,
            headers=headers
        )
        
        # Enviar most profit
        profit_csv = self.generate_top1_csv(most_profit, "profit_sum")
        profit_dto = TransactionItemBatchDTO(profit_csv, BatchType.RAW_CSV)
        
        self.output_middleware.send(
            profit_dto.to_bytes_fast(),
            routing_key=profit_routing_key,
            headers=headers
        )
        profit_eof = TransactionItemBatchDTO("EOF:1", BatchType.EOF)
        self.output_middleware.send(
            profit_eof.to_bytes_fast(),
            routing_key=profit_routing_key,
            headers=headers
        )
        
        logger.info(f"Resultados finales enviados al JOIN para cliente {client_id}")
        logger.info(f"  Best selling: {len(best_selling)} meses")
        logger.info(f"  Most profit: {len(most_profit)} meses")
    
    def process_message(self, message: bytes, routing_key: str, headers: dict = None) -> bool:
        try:
            if self.shutdown.is_shutting_down():
                logger.warning("Shutdown en progreso")
                return True
            
            client_id = 'default_client'
            if headers and 'client_id' in headers:
                client_id = headers['client_id']
                if isinstance(client_id, bytes):
                    client_id = client_id.decode('utf-8')
            
            client_id = str(client_id)
            
            dto = TransactionItemBatchDTO.from_bytes_fast(message)
            
            if dto.batch_type == BatchType.EOF:
                return self.handle_eof(routing_key, client_id)
            
            if dto.batch_type == BatchType.RAW_CSV:
                for line in dto.data.split('\n'):
                    if line.strip():
                        self.process_csv_line(line.strip(), routing_key, client_id)
            
            return False
            
        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")
            return False
    
    def on_message_callback(self, ch, method, properties, body):
        try:
            if self.shutdown.is_shutting_down():
                ch.stop_consuming()
                return
            
            routing_key = getattr(method, 'routing_key', None)
            headers = getattr(properties, 'headers', None)
            
            should_stop = self.process_message(body, routing_key, headers)
            if should_stop:
                ch.stop_consuming()
                
        except Exception as e:
            logger.error(f"Error en callback: {e}")
    
    def start(self):
        try:
            logger.info("Iniciando BestSellingAggregatorFinal...")
            self.input_middleware.start_consuming(self.on_message_callback)
        except KeyboardInterrupt:
            logger.info("Detenido manualmente")
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
            logger.error(f"Error en cleanup: {e}")


if __name__ == "__main__":
    try:
        aggregator = BestSellingAggregatorNode()
        aggregator.start()
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error fatal: {e}")
        sys.exit(1)