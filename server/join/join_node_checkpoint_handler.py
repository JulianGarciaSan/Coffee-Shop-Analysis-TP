import logging
import os
import json
from typing import Dict, Any

logger = logging.getLogger(__name__)


class JoinNodeCheckpointHandler:
    def __init__(self, join_node):
        self.join_node = join_node

    def serialize_operation(self, client_id: str, csv_line: str) -> str:
        """
        Serializa una operación para el LOG.
        Formato LOG: client_id,tipo,data
        """
        try:
            if ':' not in csv_line:
                logger.warning(f"Línea sin prefijo: {csv_line[:50]}...")
                return None
            
            prefix, content = csv_line.split(':', 1)
            
            if prefix == 'EOF':
                eof_type = content
                return f"{client_id},EOF,{eof_type}"
            
            if prefix == 'SENT':
                query_name = content
                return f"{client_id},SENT,{query_name}"
            
            tipo = prefix
            csv_data = content
            
            valid_types = ['stores', 'users', 'menu_items', 'tpv', 
                        'top_customers', 'best_selling', 'most_profit']
            if tipo not in valid_types:
                logger.warning(f"Tipo inválido: {tipo}")
                return None
            
            return f"{client_id},{tipo},{csv_data}"
            
        except Exception as e:
            logger.error(f"Error serializando: {e}")
            return None


    def deserialize_operation(self, operation_str: str) -> dict:
        """
        Deserializa desde: "0,stores,1,G Coffee @ USJ 89q"
        o "0,EOF,stores"
        o "0,SENT,q3"
        """
        try:
            parts = operation_str.split(',', 2)
            
            if len(parts) < 3:
                raise ValueError(f"Formato inválido: {operation_str}")
            
            client_id, tipo, content = parts
            
            if tipo == 'EOF':
                return {
                    'client_id': client_id,
                    'tipo': 'EOF',
                    'eof_type': content,
                    'csv_data': ''
                }
            
            if tipo == 'SENT':
                return {
                    'client_id': client_id,
                    'tipo': 'SENT',
                    'query_name': content,
                    'csv_data': ''
                }
            
            return {
                'client_id': client_id,
                'tipo': tipo,
                'csv_data': content,
                'eof_type': ''
            }
        except Exception as e:
            logger.error(f"Error deserializando: {e}")
            raise


    def apply_operation(self, operation: dict):
        """
        Aplica una operación recuperada del WAL.
        """
        try:
            client_id = operation['client_id']
            tipo = operation['tipo']
            csv_data = operation['csv_data']
            
            self.join_node._get_or_create_processors(client_id)
            state = self.join_node.client_states[client_id]
            
            if tipo == 'SENT':
                query_name = operation.get('query_name', '')
                if query_name == 'q3':
                    state.q3_results_sent = True
                    logger.info(f"[WAL Recovery] Q3 results ya enviados para cliente {client_id}")
                elif query_name == 'q4':
                    state.q4_results_sent = True
                    logger.info(f"[WAL Recovery] Q4 results ya enviados para cliente {client_id}")
                elif query_name == 'best_selling':
                    state.best_selling_sent = True
                    logger.info(f"[WAL Recovery] Best selling results ya enviados para cliente {client_id}")
                elif query_name == 'most_profit':
                    state.most_profit_sent = True
                    logger.info(f"[WAL Recovery] Most profit results ya enviados para cliente {client_id}")
                else:
                    logger.warning(f"Query name desconocido: {query_name}")
                return
            
            if tipo == 'EOF':
                eof_type = operation.get('eof_type', '')
                if eof_type == 'stores':
                    state.stores_loaded = True
                    logger.info(f"[LOG Recovery] EOF stores para cliente {client_id}")
                elif eof_type == 'users':
                    state.users_loaded = True
                    logger.info(f"[LOG Recovery] EOF users para cliente {client_id}")
                elif eof_type == 'menu_items':
                    state.menu_items_loaded = True
                    logger.info(f"[LOG Recovery] EOF menu_items para cliente {client_id}")
                elif eof_type == 'tpv':
                    state.groupby_eof_count += 1
                    logger.info(f"[LOG Recovery] EOF tpv para cliente {client_id}: {state.groupby_eof_count}/{state.expected_groupby_nodes}")
                elif eof_type == 'top_customers':
                    state.top_customers_loaded = True
                    state.top_customers_eof_count += 1
                    logger.info(f"[LOG Recovery] EOF top_customers para cliente {client_id}: {state.top_customers_eof_count}/{state.expected_top_customers_aggregators}")
                elif eof_type == 'best_selling':
                    state.best_selling_loaded = True
                    logger.info(f"[LOG Recovery] EOF best_selling para cliente {client_id}")
                elif eof_type == 'most_profit':
                    state.most_profit_loaded = True
                    logger.info(f"[LOG Recovery] EOF most_profit para cliente {client_id}")
                else:
                    logger.warning(f"Tipo de EOF desconocido: {eof_type}")
                return
            
            if tipo == 'stores':
                self.join_node.store_processors[client_id].process_batch(csv_data)
            elif tipo == 'users':
                self.join_node.user_processors[client_id].process_batch(csv_data)
            elif tipo == 'menu_items':
                self.join_node.menu_item_processors[client_id].process_batch(csv_data)
            elif tipo == 'tpv':
                self.join_node.tpv_processors[client_id].process_batch(
                    csv_data, 
                    self.join_node.tpv_query_handler._parse_tpv_line
                )
            elif tipo == 'top_customers':
                self.join_node.top_customers_processors[client_id].process_batch(
                    csv_data,
                    self.join_node.top_customers_query_handler._parse_top_customers_line
                )
            elif tipo == 'best_selling':
                self.join_node.best_selling_processors[client_id].process_batch(
                    csv_data,
                    self.join_node.profit_and_selling_query_handler._parse_best_selling_line
                )
            elif tipo == 'most_profit':
                self.join_node.most_profit_processors[client_id].process_batch(
                    csv_data,
                    self.join_node.profit_and_selling_query_handler._parse_most_profit_line
                )
            else:
                logger.warning(f"Tipo desconocido: {tipo}")
                
        except Exception as e:
            logger.error(f"Error aplicando operación: {e}", exc_info=True)
            raise


    def _serialize_client_data(self, client_id: str) -> dict:
        """
        Serializa todo el estado de un cliente para checkpoint.
        """
        serialized = {}
        
        # 1. Estado
        if client_id in self.join_node.client_states:
            state = self.join_node.client_states[client_id]
            serialized['state'] = {
                'stores_loaded': state.stores_loaded,
                'users_loaded': state.users_loaded,
                'menu_items_loaded': state.menu_items_loaded,
                'top_customers_loaded': state.top_customers_loaded,
                'best_selling_loaded': state.best_selling_loaded,
                'most_profit_loaded': state.most_profit_loaded,
                'groupby_eof_count': state.groupby_eof_count,
                'expected_groupby_nodes': state.expected_groupby_nodes,
                'expected_top_customers_aggregators': state.expected_top_customers_aggregators,
                'top_customers_eof_count': state.top_customers_eof_count,
                'q3_results_sent': state.q3_results_sent,
                'q4_results_sent': state.q4_results_sent,
                'best_selling_sent': state.best_selling_sent,
                'most_profit_sent': state.most_profit_sent,
            }
        serialized['outgoing_counter'] = self.join_node.outgoing_counter_by_client.get(client_id, 0)

        if client_id in self.join_node.store_processors:
            serialized['stores'] = self.join_node.store_processors[client_id].get_data()

        if client_id in self.join_node.user_processors:
            serialized['users'] = self.join_node.user_processors[client_id].get_data()

        if client_id in self.join_node.menu_item_processors:
            serialized['menu_items'] = self.join_node.menu_item_processors[client_id].get_data()

        if client_id in self.join_node.tpv_processors:
            serialized['tpv'] = self.join_node.tpv_processors[client_id].get_data()

        if client_id in self.join_node.top_customers_processors:
            serialized['top_customers'] = self.join_node.top_customers_processors[client_id].get_data()

        if client_id in self.join_node.best_selling_processors:
            serialized['best_selling'] = self.join_node.best_selling_processors[client_id].get_data()

        if client_id in self.join_node.most_profit_processors:
            serialized['most_profit'] = self.join_node.most_profit_processors[client_id].get_data()

        if client_id in self.join_node.q3_joined_data_by_client:
            serialized['q3_joined_data'] = self.join_node.q3_joined_data_by_client[client_id]
        
        return serialized


    def _deserialize_client_data(self, client_id: str, data: dict):
        """
        Restaura todo el estado de un cliente desde checkpoint.
        """
        self.join_node._get_or_create_processors(client_id)
        
        if 'state' in data:
            state_data = data['state']
            state = self.join_node.client_states[client_id]
            
            state.stores_loaded = state_data.get('stores_loaded', False)
            state.users_loaded = state_data.get('users_loaded', False)
            state.menu_items_loaded = state_data.get('menu_items_loaded', False)
            state.top_customers_loaded = state_data.get('top_customers_loaded', False)
            state.best_selling_loaded = state_data.get('best_selling_loaded', False)
            state.most_profit_loaded = state_data.get('most_profit_loaded', False)
            state.groupby_eof_count = state_data.get('groupby_eof_count', 0)
            logger.info(f"Restaurados TPV EOF:{state.groupby_eof_count} para cliente {client_id}    ")
            state.expected_groupby_nodes = state_data.get('expected_groupby_nodes', 2)
            state.expected_top_customers_aggregators = state_data.get('expected_top_customers_aggregators', 2)
            state.top_customers_eof_count = state_data.get('top_customers_eof_count', 0)
            logger.info(f"Restaurados top_customers EOF:{state.top_customers_eof_count} para cliente {client_id}    ")
            state.q3_results_sent = state_data.get('q3_results_sent', False)
            state.q4_results_sent = state_data.get('q4_results_sent', False)
            state.best_selling_sent = state_data.get('best_selling_sent', False)
            logger.info(f"Restaurados best_selling EOF:{state.best_selling_sent} para cliente {client_id}    ")
            state.most_profit_sent = state_data.get('most_profit_sent', False)
            logger.info(f"Restaurados most_profit EOF:{state.most_profit_sent} para cliente {client_id}    ")

            
            logger.info(f"Estado restaurado para cliente {client_id}")
        
        self.join_node.outgoing_counter_by_client[client_id] = data.get('outgoing_counter', 0)

        if 'stores' in data:
            self.join_node.store_processors[client_id].data = data['stores']
            logger.info(f"Restaurados {len(data['stores'])} stores para cliente {client_id}")
        
        if 'users' in data:
            self.join_node.user_processors[client_id].data = data['users']
            logger.info(f"Restaurados {len(data['users'])} users para cliente {client_id}")
        
        if 'menu_items' in data:
            self.join_node.menu_item_processors[client_id].data = data['menu_items']
            logger.info(f"Restaurados {len(data['menu_items'])} menu items para cliente {client_id}")
        
        if 'tpv' in data:
            self.join_node.tpv_processors[client_id].data = data['tpv']
            logger.info(f"Restaurados {len(data['tpv'])} registros TPV para cliente {client_id}")
        
        if 'top_customers' in data:
            self.join_node.top_customers_processors[client_id].data = data['top_customers']
            logger.info(f"Restaurados {len(data['top_customers'])} top customers para cliente {client_id}")
        
        if 'best_selling' in data:
            self.join_node.best_selling_processors[client_id].data = data['best_selling']
            logger.info(f"Restaurados {len(data['best_selling'])} best selling para cliente {client_id}")
        
        if 'most_profit' in data:
            self.join_node.most_profit_processors[client_id].data = data['most_profit']
            logger.info(f"Restaurados {len(data['most_profit'])} most profit para cliente {client_id}")
        
        if 'q3_joined_data' in data:
            self.join_node.q3_joined_data_by_client[client_id] = data['q3_joined_data']
            logger.info(f"Restaurados {len(data['q3_joined_data'])} registros Q3 joined para cliente {client_id}")