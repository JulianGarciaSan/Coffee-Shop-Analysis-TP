import logging
import os
import sys
from typing import Dict, List
from rabbitmq.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
from dtos.dto import TransactionBatchDTO, StoreBatchDTO, UserBatchDTO, MenuItemBatchDTO, TransactionItemBatchDTO, BatchType
from common.graceful_shutdown import GracefulShutdown

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class JoinNode:
    def __init__(self):
        self.shutdown = GracefulShutdown()
        self.shutdown.register_callback(self._on_shutdown_signal)
        
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        
        self.input_exchange = os.getenv('INPUT_EXCHANGE', 'join.exchange')
        self.output_exchange = os.getenv('OUTPUT_EXCHANGE', 'report.exchange')
        
        self.input_middleware = MessageMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name='join_input_queue',
            exchange_name=self.input_exchange,
            routing_keys=['stores.data', 'tpv.data', 
                       'top_customers.data', 'top_customers.eof',
                       'users.data', 'users.eof',
                       'menu_items.data',
                       'q2_best_selling.data', 'q2_most_profit.data']
        )
        if hasattr(self.input_middleware, 'shutdown'):
            self.input_middleware.shutdown = self.shutdown
            
        self.output_middleware = MessageMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name=self.output_exchange,
            route_keys=['q3.data', 'q3.eof', 'q4.data', 'q4.eof', 
                       'q2_best_selling.data',
                       'q2_most_profit.data']
        )
        
        if hasattr(self.output_middleware, 'shutdown'):
            self.output_middleware.shutdown = self.shutdown
            
        self.stores_data: Dict[str, Dict] = {}
        self.users_data: Dict[str, Dict] = {}
        self.tpv_data: List[Dict] = []
        self.top_customers_data: List[Dict] = []
        self.joined_data: List[Dict] = []
        self.menu_items_data: Dict[str, Dict] = {}
        self.best_selling_data: List[Dict] = []
        self.most_profit_data: List[Dict] = []
        
        # Flags de carga
        self.stores_loaded = False
        self.users_loaded = False
        self.menu_items_loaded = False
        self.top_customers_loaded = False

        self.expected_groupby_nodes = 2  # Semestre 1 y 2
        self.top_customers_sent = False  # Para evitar múltiples envíos de Q4
        self.q3_results_sent = False

        self.best_selling_loaded = False
        self.most_profit_loaded = False
        
        self.groupby_eof_count = 0
        self.expected_groupby_nodes = 2
        self.top_customers_sent = False
        self.best_selling_sent = False
        self.most_profit_sent = False


        self.store_dto_helper = StoreBatchDTO("", BatchType.RAW_CSV)
        self.user_dto_helper = UserBatchDTO("", BatchType.RAW_CSV)
        self.menu_item_dto_helper = MenuItemBatchDTO("", BatchType.RAW_CSV)
        self.item_dto_helper = TransactionItemBatchDTO("", BatchType.RAW_CSV)

        logger.info(f"JoinNode inicializado:")
        logger.info(f"  Exchange: {self.input_exchange}")
        logger.info(f"  Routing keys: stores, users, menu_items, tpv, top_customers, q2_best_selling, q2_most_profit")
    
    def _on_shutdown_signal(self):
        logger.info("Señal de shutdown recibida en JoinNode")
        if self.input_middleware:
            self.input_middleware.stop_consuming()
            
    def _process_store_batch(self, csv_data: str):
        processed_count = 0
        
        for line in csv_data.split('\n'):
            line = line.strip()
            if not line:
                continue
            
            try:
                store_id = self.store_dto_helper.get_column_value(line, 'store_id')
                store_name = self.store_dto_helper.get_column_value(line, 'store_name')
                
                if store_id and store_name:
                    self.stores_data[store_id] = {
                        'store_id': store_id,
                        'store_name': store_name,
                        'raw_line': line
                    }
                    processed_count += 1
                    
            except Exception as e:
                logger.warning(f"Error procesando línea de store: {line}, error: {e}")
                continue
        
        logger.info(f"Stores procesados: {processed_count}. Total en memoria: {len(self.stores_data)}")
    
    def _process_user_batch(self, csv_data: str):
        processed_count = 0
        
        for line in csv_data.split('\n'):
            line = line.strip()
            if not line:
                continue
            
            try:
                user_id = self.user_dto_helper.get_column_value(line, 'user_id')
                birthdate = self.user_dto_helper.get_column_value(line, 'birthdate')
                gender = self.user_dto_helper.get_column_value(line, 'gender')
                registered_at = self.user_dto_helper.get_column_value(line, 'registered_at')
                
                if user_id and birthdate and user_id != 'user_id':
                    self.users_data[user_id] = {
                        'user_id': user_id,
                        'gender': gender,
                        'birth_date': birthdate,
                        'registered_at': registered_at,
                        'raw_line': line
                    }
                    processed_count += 1
                    
            except Exception as e:
                logger.warning(f"Error procesando línea de user: {line}, error: {e}")
                continue
        
        logger.info(f"Users procesados: {processed_count}. Total en memoria: {len(self.users_data)}")
    
    def _process_menu_item_batch(self, csv_data: str):
        processed_count = 0
        
        for line in csv_data.split('\n'):
            line = line.strip()
            if not line:
                continue
            
            try:
                item_id = self.menu_item_dto_helper.get_column_value(line, 'item_id')
                item_name = self.menu_item_dto_helper.get_column_value(line, 'item_name')
                if item_id and item_name:
                    self.menu_items_data[item_id] = {
                        'item_id': item_id,
                        'item_name': item_name,
                    }
                    processed_count += 1
                    
            except Exception as e:
                logger.warning(f"Error procesando línea de menu item: {line}, error: {e}")
                continue
        
        logger.info(f"Menu items procesados: {processed_count}. Total en memoria: {len(self.menu_items_data)}")
    
    def _process_best_selling_batch(self, csv_data: str):
        processed_count = 0
        
        for line in csv_data.split('\n'):
            line = line.strip()
            if not line or line.startswith('created_at'):
                continue
            
            try:
                parts = line.split(',')
                if len(parts) < 3:
                    continue
                    
                year_month = parts[0] 
                item_id = parts[1]
                sellings_qty = int(parts[2])
                
                self.best_selling_data.append({
                    'year_month_created_at': year_month,
                    'item_id': item_id,
                    'sellings_qty': sellings_qty
                })
                processed_count += 1
                    
            except (ValueError, IndexError) as e:
                logger.warning(f"Error procesando línea de best_selling: {line}, error: {e}")
                continue
        
        logger.info(f"Best_selling procesados: {processed_count}. Total en memoria: {len(self.best_selling_data)}")

    def _process_most_profit_batch(self, csv_data: str):
        processed_count = 0
        
        for line in csv_data.split('\n'):
            line = line.strip()
            if not line or line.startswith('created_at'):
                continue
            
            try:
                parts = line.split(',')
                if len(parts) < 3:
                    continue
                    
                year_month = parts[0] 
                item_id = parts[1]
                profit_sum = float(parts[2])
                
                self.most_profit_data.append({
                    'year_month_created_at': year_month,
                    'item_id': item_id,
                    'profit_sum': profit_sum
                })
                processed_count += 1
                    
            except (ValueError, IndexError) as e:
                logger.warning(f"Error procesando línea de most_profit: {line}, error: {e}")
                continue
        
        logger.info(f"Most_profit procesados: {processed_count}. Total en memoria: {len(self.most_profit_data)}")
        
    def _process_tpv_batch(self, csv_data: str):
        processed_count = 0
        
        for line in csv_data.split('\n'):
            line = line.strip()
            if not line or line.startswith('year_half_created_at'):
                continue
            
            try:
                parts = line.split(',')
                if len(parts) >= 4:
                    tpv_record = {
                        'year_half_created_at': parts[0],
                        'store_id': parts[1],
                        'total_payment_value': float(parts[2]),
                        'transaction_count': int(parts[3])
                    }
                    self.tpv_data.append(tpv_record)
                    processed_count += 1
                    
            except (ValueError, IndexError) as e:
                logger.warning(f"Error procesando línea de TPV: {line}, error: {e}")
                continue
        
        logger.info(f"TPV procesados: {processed_count}. Total en memoria: {len(self.tpv_data)}")
        
        if self.stores_loaded:
            self._perform_join()
    
    def _process_top_customers_batch(self, csv_data: str):
        processed_count = 0
        
        for line in csv_data.split('\n'):
            line = line.strip()
            if not line or line.startswith('store_id,user_id,purchases_qty'):
                continue
            
            try:
                parts = line.split(',')
                if len(parts) >= 3:
                    raw_user_id = parts[1]
                    if '.' in raw_user_id and raw_user_id.endswith('.0'):
                        user_id = raw_user_id[:-2]
                    else:
                        user_id = raw_user_id
                    
                    top_customer_record = {
                        'store_id': parts[0],
                        'user_id': user_id,
                        'purchases_qty': int(parts[2])
                    }
                    self.top_customers_data.append(top_customer_record)
                    processed_count += 1
                    
            except (ValueError, IndexError) as e:
                logger.warning(f"Error procesando línea de top customers: {line}, error: {e}")
                continue
        
        logger.info(f"Top customers procesados: {processed_count}. Total en memoria: {len(self.top_customers_data)}")
        
        if self.stores_loaded and not self.top_customers_sent:
            self._perform_top_customers_join()
            self.top_customers_sent = True
    
    def _perform_join(self):
        if not self.stores_data or not self.tpv_data:
            logger.warning("No hay datos suficientes para JOIN Q3")
            return
        
        self.joined_data.clear() 
        joined_count = 0
        missing_stores = set()
        
        for tpv_record in self.tpv_data:
            store_id = tpv_record['store_id']
            
            if store_id in self.stores_data:
                store_name = self.stores_data[store_id]['store_name']
            else:
                missing_stores.add(store_id)
                store_name = f"Store_{store_id}"
            
            joined_record = {
                'year_half_created_at': tpv_record['year_half_created_at'],
                'store_id': store_id,
                'store_name': store_name,
                'tpv': tpv_record['total_payment_value']
            }
            
            self.joined_data.append(joined_record)
            joined_count += 1
        
        if missing_stores:
            logger.warning(f"Stores no encontrados: {missing_stores}")
        
        logger.info(f"JOIN Q3 completado: {joined_count} registros en memoria")

    def _perform_top_customers_join(self):
        if not self.top_customers_data:
            logger.warning("No hay datos de top customers para hacer JOIN")
            return
        if not self.users_data:
            logger.warning("No hay datos de users para hacer JOIN de top customers")
        if not self.stores_data or not self.top_customers_data or not self.users_data:
            logger.warning("No hay datos suficientes para JOIN Q4")
            return
        
        joined_top_customers = []
        joined_count = 0
        missing_users = set()
        missing_stores = set()  
        found_users = 0

        for customer_record in self.top_customers_data:
            store_id = customer_record['store_id']
            user_id = customer_record['user_id']
            
            if self.stores_loaded and store_id in self.stores_data:
                store_name = self.stores_data[store_id]['store_name']
            else:
                missing_stores.add(store_id)  
                store_name = f"Store_{store_id}"  
            
            if user_id in self.users_data:
                birthdate = self.users_data[user_id]['birth_date']
            else:
                missing_users.add(user_id)
                birthdate = 'UNKNOWN'
            
            joined_record = {
                'store_id': store_id,
                'store_name': store_name,
                'birthdate': birthdate
            }
            
            joined_top_customers.append(joined_record)
            joined_count += 1
        

        if missing_stores:
            logger.warning(f"Stores no encontrados para Q4: {missing_stores}")
        if missing_users:
            logger.warning(f"Users no encontrados para Q4 (primeros 10): {list(missing_users)[:10]}")
        
        logger.info(f"JOIN Q4 completado: {joined_count} registros")
        self._send_top_customers_results(joined_top_customers)
    
    def _perform_best_selling_join(self):
        if not self.menu_items_data or not self.best_selling_data:
            logger.warning("No hay datos suficientes para JOIN Q2 best_selling")
            return
        
        joined_best_selling = []
        missing_items = set()
        
        for record in self.best_selling_data:
            item_id = record['item_id']
            
            if item_id in self.menu_items_data:
                item_name = self.menu_items_data[item_id]['item_name']
            else:
                missing_items.add(item_id)
                item_name = f"Item_{item_id}"
            
            joined_record = {
                'year_month_created_at': record['year_month_created_at'],
                'item_name': item_name,
                'sellings_qty': record['sellings_qty']
            }
            
            joined_best_selling.append(joined_record)
        
        if missing_items:
            logger.warning(f"Items no encontrados para Q2 best_selling: {missing_items}")
        
        logger.info(f"JOIN Q2 best_selling completado: {len(joined_best_selling)} registros")
        self._send_best_selling_results(joined_best_selling)
    
    def _perform_most_profit_join(self):
        if not self.menu_items_data or not self.most_profit_data:
            logger.warning("No hay datos suficientes para JOIN Q2 most_profit")
            return
        
        joined_most_profit = []
        missing_items = set()
        
        for record in self.most_profit_data:
            item_id = record['item_id']
            
            if item_id in self.menu_items_data:
                item_name = self.menu_items_data[item_id]['item_name']
            else:
                missing_items.add(item_id)
                item_name = f"Item_{item_id}"
            
            joined_record = {
                'year_month_created_at': record['year_month_created_at'],
                'item_name': item_name,
                'profit_sum': record['profit_sum']
            }
            
            joined_most_profit.append(joined_record)
        
        if missing_items:
            logger.warning(f"Items no encontrados para Q2 most_profit: {missing_items}")
        
        logger.info(f"JOIN Q2 most_profit completado: {len(joined_most_profit)} registros")
        self._send_most_profit_results(joined_most_profit)
    
    def _handle_store_eof(self):
        logger.info("EOF recibido de stores")
        self.stores_loaded = True
        
        if self.tpv_data and self.groupby_eof_count >= self.expected_groupby_nodes and not self.q3_results_sent:

            self._perform_join()
            logger.info("Stores y GroupBy completados - enviando Q3")
            self._send_results_to_exchange()
            self.q3_results_sent = True  
        
        if self.top_customers_data and self.users_loaded and not self.top_customers_sent:
            logger.info("Stores y users cargados - enviando Q4")
            self._perform_top_customers_join()
            self.top_customers_sent = True

        return False
    
    def _handle_user_eof(self):
        logger.info("EOF recibido de users")
        self.users_loaded = True
        
        if self.top_customers_data and self.stores_loaded and not self.top_customers_sent:
            logger.info("Users, stores y top_customers completados - enviando Q4")
            self._perform_top_customers_join()
            self.top_customers_sent = True
        
        return False
    
    def _handle_menu_items_eof(self):
        logger.info("EOF recibido de menu_items")
        self.menu_items_loaded = True
        
        if self.best_selling_loaded and not self.best_selling_sent:
            logger.info("Menu items y best_selling completados - haciendo JOIN Q2 best_selling")
            self._perform_best_selling_join()
            self.best_selling_sent = True
            
        if self.most_profit_loaded and not self.most_profit_sent:
            logger.info("Menu items y most_profit completados - haciendo JOIN Q2 most_profit")
            self._perform_most_profit_join()
            self.most_profit_sent = True
        
        return False
    
    def _handle_tpv_eof(self):
        self.groupby_eof_count += 1
        logger.info(f"EOF TPV recibido: {self.groupby_eof_count}/{self.expected_groupby_nodes}")
        
        if self.groupby_eof_count >= self.expected_groupby_nodes and not self.q3_results_sent:

            if self.stores_loaded:
                if not self.joined_data:
                    self._perform_join()
                logger.info("Todos los GroupBy y stores completados - enviando Q3")
                self._send_results_to_exchange()
                self.q3_results_sent = True 
            else:
                logger.info("Todos los GroupBy completados, esperando stores...")
        
        return False
    
    def _handle_top_customers_eof(self):
        logger.info("EOF recibido de top_customers")
        self.top_customers_loaded = True
        

        if self.stores_loaded and self.users_loaded and not self.top_customers_sent:
            logger.info("Top customers, stores y users completados - enviando Q4")
            self._perform_top_customers_join()
            self.top_customers_sent = True
        
        return False
    
    def _handle_best_selling_eof(self):
        logger.info("EOF recibido de best_selling")
        self.best_selling_loaded = True
        
        if self.menu_items_loaded and not self.best_selling_sent:
            logger.info("Menu items y best_selling completados - haciendo JOIN Q2 best_selling")
            self._perform_best_selling_join()
            self.best_selling_sent = True
        else:
            logger.info("Best_selling completado, esperando menu_items")
        
        return False
    
    def _handle_most_profit_eof(self):
        logger.info("EOF recibido de most_profit")
        self.most_profit_loaded = True
        
        if self.menu_items_loaded and not self.most_profit_sent:
            logger.info("Menu items y most_profit completados - haciendo JOIN Q2 most_profit")
            self._perform_most_profit_join()
            self.most_profit_sent = True
        else:
            missing = []
            if not self.users_loaded:
                missing.append("users")
            logger.info(f"Top customers completado, esperando EOF de: {', '.join(missing)} para enviar Q4")
        
        return False  

    
    def _send_results_to_exchange(self):
        try:
            if not self.joined_data:
                logger.warning("No hay datos joinados para Q3")
                return
            
            sorted_joined_data = sorted(self.joined_data, key=lambda x: (x['year_half_created_at'], int(x['store_id'])))
            
            csv_lines = ["year_half_created_at,store_name,tpv"]
            for record in sorted_joined_data:
                csv_lines.append(f"{record['year_half_created_at']},{record['store_name']},{record['tpv']:.1f}")
            
            results_csv = '\n'.join(csv_lines)
            
            result_dto = TransactionBatchDTO(results_csv, BatchType.RAW_CSV)
            self.output_middleware.send(result_dto.to_bytes_fast(), routing_key='q3.data')
            
            eof_dto = TransactionBatchDTO("EOF:1", BatchType.EOF)
            self.output_middleware.send(eof_dto.to_bytes_fast(), routing_key='q3.eof')
            
            logger.info(f"Resultados Q3 enviados: {len(self.joined_data)} registros")
            
        except Exception as e:
            logger.error(f"Error enviando resultados Q3: {e}")
    
    def _send_top_customers_results(self, joined_top_customers):
        try:
            if not joined_top_customers:
                logger.warning("No hay datos para Q4")
                return
            
            sorted_top_customers = sorted(joined_top_customers, key=lambda x: int(x['store_id']))
            
            csv_lines = ["store_name,birthdate"]
            for record in sorted_top_customers:
                csv_lines.append(f"{record['store_name']},{record['birthdate']}")
            
            results_csv = '\n'.join(csv_lines)
            
            result_dto = TransactionBatchDTO(results_csv, BatchType.RAW_CSV)
            self.output_middleware.send(result_dto.to_bytes_fast(), routing_key='q4.data')
            
            eof_dto = TransactionBatchDTO("EOF:1", BatchType.EOF)
            self.output_middleware.send(eof_dto.to_bytes_fast(), routing_key='q4.eof')
            
            logger.info(f"Resultados Q4 enviados: {len(joined_top_customers)} registros")
            
        except Exception as e:
            logger.error(f"Error enviando resultados Q4: {e}")
    
    def _send_best_selling_results(self, joined_best_selling):
        try:
            if not joined_best_selling:
                logger.warning("No hay datos para Q2 best_selling")
                return
            
            sorted_data = sorted(joined_best_selling, key=lambda x: x['year_month_created_at'])
            
            csv_lines = ["year_month_created_at,item_name,sellings_qty"]
            for record in sorted_data:
                csv_lines.append(f"{record['year_month_created_at']},{record['item_name']},{record['sellings_qty']}")
            
            results_csv = '\n'.join(csv_lines)
            
            result_dto = TransactionItemBatchDTO(results_csv, BatchType.RAW_CSV)
            self.output_middleware.send(result_dto.to_bytes_fast(), routing_key='q2_best_selling.data')
            
            eof_dto = TransactionItemBatchDTO("EOF:1", BatchType.EOF)
            self.output_middleware.send(eof_dto.to_bytes_fast(), routing_key='q2_best_selling.data')
            
            logger.info(f"Resultados Q2 best_selling enviados: {len(joined_best_selling)} registros")
            
        except Exception as e:
            logger.error(f"Error enviando resultados Q2 best_selling: {e}")
    
    def _send_most_profit_results(self, joined_most_profit):
        try:
            if not joined_most_profit:
                logger.warning("No hay datos para Q2 most_profit")
                return
            
            sorted_data = sorted(joined_most_profit, key=lambda x: x['year_month_created_at'])
            
            csv_lines = ["year_month_created_at,item_name,profit_sum"]
            for record in sorted_data:
                csv_lines.append(f"{record['year_month_created_at']},{record['item_name']},{record['profit_sum']:.1f}")
            
            results_csv = '\n'.join(csv_lines)
            
            result_dto = TransactionItemBatchDTO(results_csv, BatchType.RAW_CSV)
            self.output_middleware.send(result_dto.to_bytes_fast(), routing_key='q2_most_profit.data')
            
            eof_dto = TransactionItemBatchDTO("EOF:1", BatchType.EOF)
            self.output_middleware.send(eof_dto.to_bytes_fast(), routing_key='q2_most_profit.data')
            
            logger.info(f"Resultados Q2 most_profit enviados: {len(joined_most_profit)} registros")
            
        except Exception as e:
            logger.error(f"Error enviando resultados Q2 most_profit: {e}")
    
    def process_message(self, message: bytes, routing_key: str) -> bool:
        if self.shutdown.is_shutting_down():
            logger.warning("Shutdown en progreso, ignorando mensaje")
            return True
        
        try:
            if routing_key == 'stores.data':
                dto = StoreBatchDTO.from_bytes_fast(message)
                if dto.batch_type == BatchType.EOF:
                    return self._handle_store_eof()
                if dto.batch_type == BatchType.RAW_CSV:
                    self._process_store_batch(dto.data)
                    
            elif routing_key in ['users.data', 'users.eof']:
                dto = UserBatchDTO.from_bytes_fast(message)
                if dto.batch_type == BatchType.EOF:
                    return self._handle_user_eof()
                if dto.batch_type == BatchType.RAW_CSV:
                    self._process_user_batch(dto.data)
                    
            elif routing_key == 'menu_items.data':
                logger.info("Recibido batch de menu_items")
                dto = MenuItemBatchDTO.from_bytes_fast(message)
                if dto.batch_type == BatchType.EOF:
                    return self._handle_menu_items_eof()
                if dto.batch_type == BatchType.RAW_CSV:
                    self._process_menu_item_batch(dto.data)
                    
            elif routing_key in ['tpv.data']:
                dto = TransactionBatchDTO.from_bytes_fast(message)
                if dto.batch_type == BatchType.EOF:
                    return self._handle_tpv_eof()
                if dto.batch_type == BatchType.RAW_CSV:
                    self._process_tpv_batch(dto.data)
            
            elif routing_key in ['top_customers.data', 'top_customers.eof']:
                dto = TransactionBatchDTO.from_bytes_fast(message)
                if dto.batch_type == BatchType.EOF:
                    return self._handle_top_customers_eof()
                if dto.batch_type == BatchType.RAW_CSV:
                    self._process_top_customers_batch(dto.data)
            
            elif routing_key == 'q2_best_selling.data':
                dto = TransactionItemBatchDTO.from_bytes_fast(message)
                if dto.batch_type == BatchType.EOF:
                    return self._handle_best_selling_eof()
                if dto.batch_type == BatchType.RAW_CSV:
                    self._process_best_selling_batch(dto.data)
            
            elif routing_key == 'q2_most_profit.data':
                dto = TransactionItemBatchDTO.from_bytes_fast(message)
                if dto.batch_type == BatchType.EOF:
                    return self._handle_most_profit_eof()
                if dto.batch_type == BatchType.RAW_CSV:
                    self._process_most_profit_batch(dto.data)
            
            return False
            
        except Exception as e:
            logger.error(f"Error procesando mensaje con routing key {routing_key}: {e}")
            return False
    
    def on_message_callback(self, ch, method, properties, body):
        try:
            if self.shutdown.is_shutting_down():
                logger.warning("Shutdown solicitado, deteniendo")
                ch.stop_consuming()
                return
            
            routing_key = method.routing_key
            should_stop = self.process_message(body, routing_key)
            if should_stop:
                logger.info("JOIN completado - deteniendo consuming")
                ch.stop_consuming()
        except Exception as e:
            logger.error(f"Error en callback: {e}")
    
    def start(self):
        try:
            logger.info("Iniciando JoinNode...")
            self.input_middleware.start_consuming(self.on_message_callback)
            
        except KeyboardInterrupt:
            logger.info("JoinNode detenido manualmente")
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
    
    def get_joined_data(self) -> List[Dict]:
        return self.joined_data.copy()


if __name__ == "__main__":
    try:
        node = JoinNode()
        node.start()
        logger.info("JoinNode terminado exitosamente")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error fatal: {e}")
        sys.exit(1) 