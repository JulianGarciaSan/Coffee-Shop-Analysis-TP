import logging
import os
import sys
import time
from typing import Any, Dict, Optional, Tuple
from collections import defaultdict
from rabbitmq.middleware import MessageMiddlewareExchangeManual, MessageMiddlewareQueueManual
from dtos.dto import TransactionItemBatchDTO, BatchType
from common.graceful_shutdown import GracefulShutdown
from client_routing.client_routing import ClientRouter
from checkpoint_handler.checkpoint_handler import CheckpointHandler
from healthchecker.healthchecker import HealthChecker


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
        self.node_id = int(os.getenv('AGGREGATOR_NODE_ID', '0'))
        self.expected_sources = self.total_years * self.total_groupby_nodes_per_year
        self.checkpoint_dir = os.getenv('CHECKPOINT_DIR', f'/app/server/logs/best_selling_aggregator_final/checkpoints')
        total_join_nodes = int(os.getenv('TOTAL_JOIN_NODES', '1'))
        self.client_router = ClientRouter(total_join_nodes, node_prefix="join_node")
        
        self.month_selling_candidates_by_client: Dict[str, Dict[str, list]] = defaultdict(
            lambda: defaultdict(list)
        )
        self.month_profit_candidates_by_client: Dict[str, Dict[str, list]] = defaultdict(
            lambda: defaultdict(list)
        )
        self.outgoing_counter_by_client: Dict[str, int] = defaultdict(int)
        health_port = int(os.getenv('HEALTH_PORT', '9999'))
        self.health_server = HealthChecker(port=health_port)
        self.health_server.start()
        
        self.checkpoint_handler = CheckpointHandler(
            checkpoint_dir=self.checkpoint_dir,
            strategy=self,
            checkpont_interval=4,
            outgoing_counter_by_client=self.outgoing_counter_by_client,
            extra_id=int(self.node_id)
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
        
        self.input_middleware = MessageMiddlewareQueueManual(
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
        
        self.output_middleware = MessageMiddlewareExchangeManual(
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
            
            for i, cand in enumerate(candidates):
                logger.info(f"  Candidato {i}: item_id='{cand['item_id']}', sellings_qty={cand['sellings_qty']}")
            
            top = max(candidates, 
                    key=lambda x: (x['sellings_qty'], -int(x['item_id']) if x['item_id'].isdigit() else 0))
            
            best_selling[year_month] = (top['item_id'], top['sellings_qty'])
        
        profit_candidates = self.month_profit_candidates_by_client.get(client_id, {})
        for year_month, candidates in profit_candidates.items():
            if not candidates:
                continue
            
            for i, cand in enumerate(candidates):
                logger.info(f"  Candidato {i}: item_id='{cand['item_id']}', profit_sum={cand['profit_sum']}")
            
            top = max(candidates,
                    key=lambda x: (x['profit_sum'], -int(x['item_id']) if x['item_id'].isdigit() else 0))
            
            most_profit[year_month] = (top['item_id'], top['profit_sum'])
        
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
    
    def handle_eof(self, routing_key: str, client_id: str, message_id: str) -> bool:
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
                logger.info(f"✓ TODOS los EOFs recibidos para cliente {client_id} ({selling_count} selling + {profit_count} profit)")
                logger.info(f"Calculando top 1 global y enviando resultados")
                
                self._send_final_results(client_id, message_id)
                
                self.checkpoint_handler.save_eof_checkpoint(client_id, message_id)
                
                # Cleanup
                del self.eof_selling_count_by_client[client_id]
                del self.eof_profit_count_by_client[client_id]
                del self.month_selling_candidates_by_client[client_id]
                del self.month_profit_candidates_by_client[client_id]
                
                logger.info(f"Cliente {client_id} completado y limpiado")
            else:
                logger.info(f"Esperando más EOFs: {selling_count}/{self.expected_sources} selling, {profit_count}/{self.expected_sources} profit")
            
            return False
            
        except Exception as e:
            logger.error(f"Error manejando EOF: {e}")
            return False
        
    def create_headers(self, client_id: Optional[int], message_id: Optional[int]) -> Dict[str, Any]:
        headers = {}
        if client_id is not None and message_id is not None:
            return {'client_id': client_id,
                    'message_id': message_id
                    }
        return {}

    def generate_next_message_id(self, client_id: str) -> int:
        """
        Genera ID único por mensaje para este cliente.
        """
        self.outgoing_counter_by_client[client_id] += 1
        return int(self.node_id) * 1000000 + self.outgoing_counter_by_client[client_id]
    
    def _send_final_results(self, client_id: str, original_message_id: str):
        """Calcula top 1 global y envía al JOIN"""
        best_selling, most_profit = self.calculate_global_top1(client_id)        
        selling_routing_key = self.client_router.get_routing_key(client_id, 'q2_best_selling.data')
        profit_routing_key = self.client_router.get_routing_key(client_id, 'q2_most_profit.data')
        
        self.send_best_selling_data(client_id, best_selling, original_message_id, selling_routing_key)
        
        self.send_most_profit_data(client_id, most_profit, original_message_id, profit_routing_key)
        
        logger.info(f"Resultados finales enviados al JOIN para cliente {client_id}")
        
    def send_best_selling_data(self, client_id: str, best_selling: Dict[str, Tuple[str, int]], message_id: str, selling_routing_key: str):
        # Enviar best selling DATA
        selling_csv = self.generate_top1_csv(best_selling, "sellings_qty")
        unique_id = self.generate_next_message_id(client_id) 
        headers = self.create_headers(client_id, unique_id)
        
        selling_dto = TransactionItemBatchDTO(selling_csv, BatchType.RAW_CSV)
        logger.info(f"Enviando best selling (ID={unique_id}): {len(selling_csv)} bytes")
        self.output_middleware.send(
            selling_dto.to_bytes_fast(),
            routing_key=selling_routing_key,
            headers=headers
        )
        self.checkpoint_handler.save_counter_update(client_id, self.outgoing_counter_by_client[client_id],"")

        # print("///////////// ENVIANDO EOF /////////////")
        # time.sleep(10)
        # Enviar best selling EOF
        unique_id = self.generate_next_message_id(client_id)  
        headers = self.create_headers(client_id, unique_id)
        selling_eof = TransactionItemBatchDTO("EOF:1", BatchType.EOF)
        self.output_middleware.send(
            selling_eof.to_bytes_fast(),
            routing_key=selling_routing_key,
            headers=headers
        )
        logger.info(f"Enviando best selling EOF (ID={unique_id})")
        self.checkpoint_handler.save_counter_update(client_id, self.outgoing_counter_by_client[client_id],"")

        # print("///////////// ENVIANDO EOF /////////////")
        # time.sleep(10)
        
    def send_most_profit_data(self, client_id: str, most_profit: Dict[str, Tuple[str, float]], message_id: str, profit_routing_key: str):
        # Enviar most profit DATA
        unique_id = self.generate_next_message_id(client_id)
        profit_csv = self.generate_top1_csv(most_profit, "profit_sum")
        headers = self.create_headers(client_id, unique_id)
        
        profit_dto = TransactionItemBatchDTO(profit_csv, BatchType.RAW_CSV)
        self.checkpoint_handler.save_counter_update(client_id, self.outgoing_counter_by_client[client_id],"")
        logger.info(f"Enviando most profit (ID={unique_id}): {len(profit_csv)} bytes")
        self.output_middleware.send(
            profit_dto.to_bytes_fast(),
            routing_key=profit_routing_key,
            headers=headers
        )
        print("///////////// ENVIANDO EOF /////////////")
        time.sleep(10)
        # Enviar most profit EOF
        unique_id = self.generate_next_message_id(client_id)  
        headers = self.create_headers(client_id, unique_id)
        profit_eof = TransactionItemBatchDTO("EOF:1", BatchType.EOF)
        self.output_middleware.send(
            profit_eof.to_bytes_fast(),
            routing_key=profit_routing_key,
            headers=headers
        )
        logger.info(f"Enviando most profit EOF (ID={unique_id})")
        time.sleep(10)
        self.checkpoint_handler.save_counter_update(client_id, self.outgoing_counter_by_client[client_id],"")
        
    def process_message(self, message: bytes, routing_key: str, client_id: str, message_id: str) -> bool:
        try:
            if self.shutdown.is_shutting_down():
                logger.warning("Shutdown en progreso")
                return (True, False)
            
            dto = TransactionItemBatchDTO.from_bytes_fast(message)
            
            if dto.batch_type == BatchType.EOF:
                # Manejar EOF (actualiza contadores)
                self.handle_eof(routing_key, client_id, message_id)
                
                # Guardar checkpoint REGULAR (sin eof_processed=True)
                # Solo para persistir los contadores de EOF
                lines = [f"EOF:{routing_key}"]
                self.checkpoint_handler.save_message_checkpoint(
                    client_id=client_id,
                    message_id=message_id,
                    csv_lines=lines
                )
                
                return (False, True) 
            
            if dto.batch_type == BatchType.RAW_CSV:
                lines = [line.strip() for line in dto.data.split('\n') if line.strip()]
                
                for line in lines:
                    self.process_csv_line(line, routing_key, client_id)
                
                self.checkpoint_handler.save_message_checkpoint(
                    client_id=client_id,
                    message_id=message_id,
                    csv_lines=lines
                )
                return (False, True) 
            
        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")
            return (False, False)

    def parse_message_headers(self, properties) -> Tuple[str, str]:
        client_id = None
        message_id = None
        if properties and properties.headers:
            client_id = properties.headers.get('client_id')
            message_id = properties.headers.get('message_id')
        return str(client_id), str(message_id)
    
    def on_message_callback(self, ch, method, properties, body):
        try:
            if self.shutdown.is_shutting_down():
                ch.stop_consuming()
                return
            
            routing_key = getattr(method, 'routing_key', None)

            client_id, message_id = self.parse_message_headers(properties)
            
            if self.checkpoint_handler.analyze_first_message(client_id, message_id, ch, method, body):
                return
            
            # self.checkpoint_handler.register_incoming_message(client_id, message_id)
            should_stop, should_ack = self.process_message(body,routing_key, client_id, message_id)
            if should_ack:
                ch.basic_ack(delivery_tag=method.delivery_tag)
            if should_stop:
                logger.info("Shutdown solicitado - deteniendo consuming")
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
            
            
    def serialize_operation(self, client_id: str, csv_line: str) -> str:
        """
        Serializa una operación BestSelling para el WAL.
        
        Formato para selling: client_id,S,year_month,item_id,sellings_qty
        Formato para profit:  client_id,P,year_month,item_id,profit_sum
        Formato para EOF:     client_id,EOF,routing_key
        
        Ejemplo: 0,S,2024-01,item_123,50
                0,P,2024-01,item_456,125.50
                0,EOF,top_selling.data
                0,COUNTER,5
        """
        try:
            # Manejar COUNTER
            if csv_line.startswith(f'{client_id},COUNTER,'):
                parts = csv_line.split(',')
                counter_value = parts[2]
                return f"{client_id},COUNTER,{counter_value}"
        
            # Manejar EOF
            if csv_line.startswith('EOF:'):
                routing_key = csv_line.split(':', 1)[1]
                return f"{client_id},EOF,{routing_key}"
            
            parts = csv_line.split(',')
            
            # Saltear header
            if len(parts) < 3 or parts[0] == 'created_at':
                return None
            
            year_month = parts[0]
            item_id = parts[1]
            value_str = parts[2]
            
            # Detectar tipo (selling o profit)
            try:
                if '.' not in value_str or value_str.endswith('.0'):
                    value = int(float(value_str))
                    return f"{client_id},S,{year_month},{item_id},{value}"
                else:
                    value = float(value_str)
                    return f"{client_id},P,{year_month},{item_id},{value}"
            except ValueError:
                logger.warning(f"Error parseando valor '{value_str}'")
                return None
            
        except Exception as e:
            logger.error(f"Error serializando operación BestSelling: {e}")
            raise

    def deserialize_operation(self, operation_str: str) -> dict:
        """
        Deserializa una operación BestSelling desde el WAL.
        
        Input: "0,S,2024-01,item_123,50"
            "0,EOF,top_selling.data"
        """
        try:
            parts = operation_str.split(',')
            
            if len(parts) < 3:
                raise ValueError(f"Formato inválido: {operation_str}")
            
            client_id = parts[0]
            op_type = parts[1]
            if op_type == 'COUNTER':
                counter_value = int(parts[2])
                return {
                    'client_id': client_id,
                    'type': 'counter',
                    'counter_value': counter_value
                }
            # Manejar EOF
            if op_type == 'EOF':
                routing_key = parts[2]
                return {
                    'client_id': client_id,
                    'type': 'eof',
                    'routing_key': routing_key
                }
            
            # Manejar datos
            if len(parts) != 5:
                raise ValueError(f"Formato inválido, esperaba 5 campos: {operation_str}")
            
            year_month = parts[2]
            item_id = parts[3]
            value_str = parts[4]
            
            if op_type == 'S':
                return {
                    'client_id': client_id,
                    'type': 'selling',
                    'year_month': year_month,
                    'item_id': item_id,
                    'value': int(value_str)
                }
            elif op_type == 'P':
                return {
                    'client_id': client_id,
                    'type': 'profit',
                    'year_month': year_month,
                    'item_id': item_id,
                    'value': float(value_str)
                }
            else:
                raise ValueError(f"Tipo de operación desconocido: {op_type}")
            
        except Exception as e:
            logger.error(f"Error deserializando operación BestSelling: {e}")
            raise

    def apply_operation(self, operation: dict):
        """
        Aplica una operación BestSelling al estado en memoria.
        """
        try:
            client_id = operation['client_id']
            op_type = operation['type']
            if op_type == 'counter':
                counter_value = operation['counter_value']
                self.outgoing_counter_by_client[client_id] = counter_value
                logger.debug(f"[WAL Recovery] Contador restaurado: {counter_value} para cliente {client_id}")
            
            if op_type == 'eof':
                # Actualizar contador de EOF
                routing_key = operation['routing_key']
                if 'top_selling' in routing_key:
                    self.eof_selling_count_by_client[client_id] += 1
                    logger.debug(f"Aplicado EOF selling para cliente {client_id}")
                elif 'top_profit' in routing_key:
                    self.eof_profit_count_by_client[client_id] += 1
                    logger.debug(f"Aplicado EOF profit para cliente {client_id}")
            
            elif op_type == 'selling':
                year_month = operation['year_month']
                item_id = operation['item_id']
                value = operation['value']
                
                self.month_selling_candidates_by_client[client_id][year_month].append({
                    'item_id': item_id,
                    'sellings_qty': value
                })
                logger.debug(f"Aplicada operación selling: {year_month}, item {item_id}, qty {value}")
                
            elif op_type == 'profit':
                year_month = operation['year_month']
                item_id = operation['item_id']
                value = operation['value']
                
                self.month_profit_candidates_by_client[client_id][year_month].append({
                    'item_id': item_id,
                    'profit_sum': value
                })
                logger.debug(f"Aplicada operación profit: {year_month}, item {item_id}, profit ${value:.2f}")
            
        except Exception as e:
            logger.error(f"Error aplicando operación BestSelling: {e}")
            raise

    def _serialize_client_data(self, client_id: str) -> Dict:
        """
        Serializa el estado completo del cliente para checkpoint.
        
        Estructura:
        {
            "selling_candidates": {
                "2024-01": [
                    {"item_id": "item_123", "sellings_qty": 50},
                    {"item_id": "item_456", "sellings_qty": 30}
                ],
                "2024-02": [...]
            },
            "profit_candidates": {
                "2024-01": [
                    {"item_id": "item_123", "profit_sum": 125.50},
                    {"item_id": "item_456", "profit_sum": 89.20}
                ]
            },
            "eof_selling_count": 4,
            "eof_profit_count": 4,
            "outgoing_counter": 0
        }
        """
        selling_candidates = self.month_selling_candidates_by_client.get(client_id, {})
        profit_candidates = self.month_profit_candidates_by_client.get(client_id, {})
        
        serialized = {
            "selling_candidates": {},
            "profit_candidates": {},
            "eof_selling_count": self.eof_selling_count_by_client.get(client_id, 0),
            "eof_profit_count": self.eof_profit_count_by_client.get(client_id, 0),
            "outgoing_counter": self.outgoing_counter_by_client.get(client_id, 0)
        }
        
        # Serializar candidatos de selling
        for year_month, candidates in selling_candidates.items():
            serialized["selling_candidates"][year_month] = [
                {
                    "item_id": cand["item_id"],
                    "sellings_qty": cand["sellings_qty"]
                }
                for cand in candidates
            ]
        
        # Serializar candidatos de profit
        for year_month, candidates in profit_candidates.items():
            serialized["profit_candidates"][year_month] = [
                {
                    "item_id": cand["item_id"],
                    "profit_sum": cand["profit_sum"]
                }
                for cand in candidates
            ]
        
        return serialized

    def _deserialize_client_data(self, client_id: str, data: Dict):
        """
        Reconstruye el estado desde un checkpoint.
        """
        # Inicializar estructuras
        self.month_selling_candidates_by_client[client_id] = defaultdict(list)
        self.month_profit_candidates_by_client[client_id] = defaultdict(list)
        
        # Restaurar candidatos de selling
        selling_data = data.get("selling_candidates", {})
        for year_month, candidates in selling_data.items():
            self.month_selling_candidates_by_client[client_id][year_month] = [
                {
                    "item_id": cand["item_id"],
                    "sellings_qty": cand["sellings_qty"]
                }
                for cand in candidates
            ]
        
        # Restaurar candidatos de profit
        profit_data = data.get("profit_candidates", {})
        for year_month, candidates in profit_data.items():
            self.month_profit_candidates_by_client[client_id][year_month] = [
                {
                    "item_id": cand["item_id"],
                    "profit_sum": cand["profit_sum"]
                }
                for cand in candidates
            ]
        
        # Restaurar contadores de EOF
        self.eof_selling_count_by_client[client_id] = data.get("eof_selling_count", 0)
        self.eof_profit_count_by_client[client_id] = data.get("eof_profit_count", 0)
        
        # Restaurar contador de mensajes salientes
        self.outgoing_counter_by_client[client_id] = data.get("outgoing_counter", 0)
        
        logger.info(f"   Candidatos selling: {sum(len(v) for v in self.month_selling_candidates_by_client[client_id].values())}")
        logger.info(f"   Candidatos profit: {sum(len(v) for v in self.month_profit_candidates_by_client[client_id].values())}")
        logger.info(f"   EOF selling: {self.eof_selling_count_by_client[client_id]}")
        logger.info(f"   EOF profit: {self.eof_profit_count_by_client[client_id]}")
        logger.info(f"   Contador saliente: {self.outgoing_counter_by_client[client_id]}")


if __name__ == "__main__":
    try:
        aggregator = BestSellingAggregatorNode()
        aggregator.start()
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error fatal: {e}")
        sys.exit(1)