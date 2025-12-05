import logging
import json
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class JoinNodeCheckpointHandler:
    def __init__(self, join_node):
        self.join_node = join_node

    def serialize_operation(self, client_id: str, csv_line: str) -> Optional[str]:
        """
        Transforma una operación de negocio o de control en una línea de texto para el WAL.
        """
        try:
            if ':' not in csv_line:
                return None
            
            prefix, content = csv_line.split(':', 1)
            
            return f"{client_id},{prefix},{content}"
            
            
        except Exception as e:
            logger.error(f"Error crítico serializando operación: {e}")
            return None

    def deserialize_operation(self, operation_str: str) -> Dict[str, Any]:
        try:
            parts = operation_str.split(',', 2)
            if len(parts) < 3:
                raise ValueError(f"Formato de log corrupto: {operation_str}")
            
            client_id, tipo, content = parts
            
            
            if tipo == 'START_TRANSACTION':
                if ',' in content:
                    cnt, query = content.split(',', 1)
                    return {'client_id': client_id, 'tipo': tipo, 'counter': int(cnt), 'query': query}
                return {'client_id': client_id, 'tipo': tipo, 'counter': int(content), 'query': ''}

            if tipo == 'COMMIT_TRANSACTION':
                return {'client_id': client_id, 'tipo': tipo, 'counter': int(content)}

            
            if tipo in ['EOF', 'SENT']:
                return {
                    'client_id': client_id,
                    'tipo': tipo,
                    'eof_type': content.strip(), # 'q3', 'stores', etc.
                    'csv_data': ''
                }
            
            return {
                'client_id': client_id,
                'tipo': tipo,
                'csv_data': content,
                'eof_type': ''
            }
            
        except Exception as e:
            logger.error(f"Error deserializando línea '{operation_str}': {e}")
            raise

    def apply_operation(self, operation: Dict[str, Any]):
        """
        Ejecuta la operación recuperada (Replay).
        Mantiene la consistencia del contador de IDs mediante lógica de Rollback.
        """
        try:
            client_id = operation['client_id']
            tipo = operation['tipo']
            csv_data = operation.get('csv_data', '')
            
            self.join_node._get_or_create_processors(client_id)
            state = self.join_node.client_states[client_id]
            
            
            if tipo == 'START_TRANSACTION':
                start_counter = operation['counter']
                self.join_node.pending_rollbacks[client_id] = start_counter
                return

            if tipo == 'COMMIT_TRANSACTION':
                end_counter = operation['counter']
                self.join_node.outgoing_counter_by_client[client_id] = end_counter
                return
            if tipo == 'SENT':
                if client_id in self.join_node.pending_rollbacks:
                    del self.join_node.pending_rollbacks[client_id]
                    logger.debug(f"[WAL] SENT detectado. Transacción completada, rollback descartado.")

                content = operation.get('eof_type', '')
                self._mark_query_as_sent(state, content)
                return
            
            if tipo == 'EOF':
                self._apply_eof(state, operation.get('eof_type', ''))
                return
            
            self._apply_business_data(client_id, tipo, csv_data)

        except Exception as e:
            logger.error(f"Error aplicando operación WAL durante recovery: {e}", exc_info=True)

    def _mark_query_as_sent(self, state, query_name: str):
        """Helper para actualizar flags de estado."""
        if query_name == 'q3': state.q3_results_sent = True
        elif query_name == 'q4': state.q4_results_sent = True
        elif 'best_selling' in query_name: state.best_selling_sent = True
        elif 'most_profit' in query_name: state.most_profit_sent = True

    def _apply_eof(self, state, eof_type: str):
        """Helper para aplicar EOFs."""
        if eof_type == 'stores': state.stores_loaded = True
        elif eof_type == 'users': state.users_loaded = True
        elif eof_type == 'menu_items': state.menu_items_loaded = True
        elif eof_type == 'tpv': state.groupby_eof_count += 1
        elif eof_type == 'top_customers': 
            state.top_customers_loaded = True
            state.top_customers_eof_count += 1
        elif eof_type == 'best_selling': state.best_selling_loaded = True
        elif eof_type == 'most_profit': state.most_profit_loaded = True

    def _apply_business_data(self, client_id: str, tipo: str, csv_data: str):
        """Delegación dinámica a los procesadores."""
        processors = {
            'stores': self.join_node.store_processors,
            'users': self.join_node.user_processors,
            'menu_items': self.join_node.menu_item_processors,
            'tpv': self.join_node.tpv_processors,
            'top_customers': self.join_node.top_customers_processors,
            'best_selling': self.join_node.best_selling_processors,
            'most_profit': self.join_node.most_profit_processors
        }
        
        parsers = {
            'tpv': self.join_node.tpv_query_handler._parse_tpv_line,
            'top_customers': self.join_node.top_customers_query_handler._parse_top_customers_line,
            'best_selling': self.join_node.profit_and_selling_query_handler._parse_best_selling_line,
            'most_profit': self.join_node.profit_and_selling_query_handler._parse_most_profit_line
        }

        if tipo in processors:
            processor = processors[tipo][client_id]
            parser = parsers.get(tipo)
            if parser:
                processor.process_batch(csv_data, parser)
            else:
                processor.process_batch(csv_data)

    def _serialize_client_data(self, client_id: str) -> dict:
        serialized = {}
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
        
        proc_map = {
            'stores': self.join_node.store_processors,
            'users': self.join_node.user_processors,
            'menu_items': self.join_node.menu_item_processors,
            'tpv': self.join_node.tpv_processors,
            'top_customers': self.join_node.top_customers_processors,
            'best_selling': self.join_node.best_selling_processors,
            'most_profit': self.join_node.most_profit_processors
        }
        for key, proc_dict in proc_map.items():
            if client_id in proc_dict:
                serialized[key] = proc_dict[client_id].get_data()
                
        if client_id in self.join_node.q3_joined_data_by_client:
            serialized['q3_joined_data'] = self.join_node.q3_joined_data_by_client[client_id]
            
        return serialized

    def _deserialize_client_data(self, client_id: str, data: dict):
        self.join_node._get_or_create_processors(client_id)
        self.join_node.outgoing_counter_by_client[client_id] = data.get("outgoing_counter", 0)
        
        if 'state' in data:
            s_data = data['state']
            st = self.join_node.client_states[client_id]
            st.stores_loaded = s_data.get('stores_loaded', False)
            st.users_loaded = s_data.get('users_loaded', False)
            st.menu_items_loaded = s_data.get('menu_items_loaded', False)
            st.top_customers_loaded = s_data.get('top_customers_loaded', False)
            st.best_selling_loaded = s_data.get('best_selling_loaded', False)
            st.most_profit_loaded = s_data.get('most_profit_loaded', False)
            st.groupby_eof_count = s_data.get('groupby_eof_count', 0)
            st.expected_groupby_nodes = s_data.get('expected_groupby_nodes', 2)
            st.expected_top_customers_aggregators = s_data.get('expected_top_customers_aggregators', 2)
            st.top_customers_eof_count = s_data.get('top_customers_eof_count', 0)
            st.q3_results_sent = s_data.get('q3_results_sent', False)
            st.q4_results_sent = s_data.get('q4_results_sent', False)
            st.best_selling_sent = s_data.get('best_selling_sent', False)
            st.most_profit_sent = s_data.get('most_profit_sent', False)

        proc_map = {
            'stores': self.join_node.store_processors,
            'users': self.join_node.user_processors,
            'menu_items': self.join_node.menu_item_processors,
            'tpv': self.join_node.tpv_processors,
            'top_customers': self.join_node.top_customers_processors,
            'best_selling': self.join_node.best_selling_processors,
            'most_profit': self.join_node.most_profit_processors
        }
        for key, proc_dict in proc_map.items():
            if key in data:
                proc_dict[client_id].data = data[key]
                
        if 'q3_joined_data' in data:
            self.join_node.q3_joined_data_by_client[client_id] = data['q3_joined_data']