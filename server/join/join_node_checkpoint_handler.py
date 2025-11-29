import logging
import os
import json
from typing import Dict, Any

logger = logging.getLogger(__name__)


class JoinNodeCheckpointHandler:
    """
    Checkpoint handler con estrategia WAL para JoinNode.
    Persiste el estado de todos los procesadores y estados de clientes.
    """
    
    def __init__(self, join_node):
        self.join_node = join_node

    def serialize_operation(self, client_id: str, csv_line: str) -> str:
        """
        Serializa una operación para el WAL.
        
        El csv_line viene con prefijo: "TIPO:csv_data"
        Ejemplo: "stores:store_1,Store Name"
        
        Formato salida: "client_id|tipo|csv_data"
        """
        try:
            # Extraer tipo del prefijo
            if ':' not in csv_line:
                logger.warning(f"Línea sin prefijo de tipo: {csv_line[:50]}...")
                return None
            
            tipo, csv_data = csv_line.split(':', 1)
            
            # Validar tipo
            valid_types = ['stores', 'users', 'menu_items', 'tpv', 
                        'top_customers', 'best_selling', 'most_profit']
            if tipo not in valid_types:
                logger.warning(f"Tipo inválido: {tipo}")
                return None
            
            # Escapar pipes
            escaped_data = csv_data.replace('|', '\\|')
            
            return f"{client_id}|{tipo}|{escaped_data}"
            
        except Exception as e:
            logger.error(f"Error serializando operación: {e}")
            return None


    def deserialize_operation(self, operation_str: str) -> dict:
        """
        Deserializa una operación desde el WAL.
        
        Input: "1|stores|store_1,Store Name"
        Output: {
            'client_id': '1',
            'tipo': 'stores',
            'csv_data': 'store_1,Store Name'
        }
        """
        try:
            parts = operation_str.split('|', 2)
            
            if len(parts) != 3:
                raise ValueError(f"Formato inválido: {operation_str}")
            
            client_id, tipo, escaped_data = parts
            csv_data = escaped_data.replace('\\|', '|')
            
            return {
                'client_id': client_id,
                'tipo': tipo,
                'csv_data': csv_data
            }
        except Exception as e:
            logger.error(f"Error deserializando operación: {e}")
            raise


    def apply_operation(self, operation: dict):
        """
        Aplica una operación recuperada del WAL.
        """
        try:
            client_id = operation['client_id']
            tipo = operation['tipo']
            csv_data = operation['csv_data']
            
            # Asegurar procesadores
            self.join_node._get_or_create_processors(client_id)
            
            # Aplicar según tipo
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
        
        # 2-8. Procesadores
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

        # 9. Q3 joined data
        if client_id in self.join_node.q3_joined_data_by_client:
            serialized['q3_joined_data'] = self.join_node.q3_joined_data_by_client[client_id]
        
        return serialized


    def _deserialize_client_data(self, client_id: str, data: dict):
        """
        Restaura todo el estado de un cliente desde checkpoint.
        """
        self.join_node._get_or_create_processors(client_id)
        
        # 1. Estado
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
            state.expected_groupby_nodes = state_data.get('expected_groupby_nodes', 2)
            state.expected_top_customers_aggregators = state_data.get('expected_top_customers_aggregators', 2)
            state.top_customers_eof_count = state_data.get('top_customers_eof_count', 0)
            state.q3_results_sent = state_data.get('q3_results_sent', False)
            state.q4_results_sent = state_data.get('q4_results_sent', False)
            state.best_selling_sent = state_data.get('best_selling_sent', False)
            state.most_profit_sent = state_data.get('most_profit_sent', False)
            
            logger.info(f"Estado restaurado para cliente {client_id}")
        
        # 2-8. Procesadores
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