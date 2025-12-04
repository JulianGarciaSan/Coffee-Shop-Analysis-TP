import logging
from collections import defaultdict
from typing import Dict, Tuple
from .base_strategy import GroupByStrategy
from configurators.tpv_aggregation import TPVAggregation

logger = logging.getLogger(__name__)


class TPVGroupByStrategy(GroupByStrategy):
    def __init__(self, semester: str):
        super().__init__()
        self.semester = semester
        self.tpv_aggregations_by_client: Dict[str, Dict[Tuple[str, str], TPVAggregation]] = defaultdict(
            lambda: defaultdict(TPVAggregation)
        )
        self.eof_received_by_client: Dict[str, bool] = {}

        logger.info(f"TPVGroupByStrategy inicializada para semestre {self.semester}")
    
    def process_csv_line(self, csv_line: str, client_id: str = 'default_client'):
        try:
            store_id = self.dto_helper.get_column_value(csv_line, 'store_id')
            created_at = self.dto_helper.get_column_value(csv_line, 'created_at')
            final_amount_str = self.dto_helper.get_column_value(csv_line, 'final_amount')
            
            if not all([store_id, created_at, final_amount_str]):
                return
            
            year = created_at[:4]
            year_half = f"{year}-H{self.semester}"
            final_amount = float(final_amount_str)
            
            key = (year_half, store_id)
            self.tpv_aggregations_by_client[client_id][key].add_transaction(final_amount)
            
        except (ValueError, IndexError) as e:
            logger.warning(f"Error procesando línea para TPV (cliente '{client_id}'): {e}")
            
        
    def generate_results_csv_for_client(self, client_id: str) -> str:
        client_aggregations = self.tpv_aggregations_by_client.get(client_id, {})
        
        if not client_aggregations:
            logger.warning(f"No hay datos TPV para cliente '{client_id}'")
            return "year_half_created_at,store_id,total_payment_value,transaction_count"
        
        csv_lines = ["year_half_created_at,store_id,total_payment_value,transaction_count"]
        
        for (year_half, store_id) in sorted(client_aggregations.keys()):
            aggregation = client_aggregations[(year_half, store_id)]
            csv_lines.append(aggregation.to_csv_line(year_half, store_id))
        
        logger.info(f"Resultados TPV generados para cliente '{client_id}': {len(client_aggregations)} grupos")
        return '\n'.join(csv_lines)
    
    
    def serialize_operation(self, client_id: str, csv_line: str) -> str:
        """
        Serializa una operación para el log.
        
        Formato: client_id,store_id,created_at,final_amount
        Ejemplo: 0,store_1,2024-01-15,150.50
        """
        try:
            store_id = self.dto_helper.get_column_value(csv_line, 'store_id')
            created_at = self.dto_helper.get_column_value(csv_line, 'created_at')
            final_amount = self.dto_helper.get_column_value(csv_line, 'final_amount')
            
            if not all([store_id, created_at, final_amount]):
                return None  
            
            return f"{client_id},{store_id},{created_at},{final_amount}"
            
        except Exception as e:
            logger.error(f"Error serializando operacion: {e}")
        raise
    
    def deserialize_operation(self, operation_str: str) -> dict:
        """
        Deserializa UNA SOLA operación desde el log.
        
        Input: "0,store_1,2024-01-15,150.50"
        Output: {
            'client_id': '0',
            'store_id': 'store_1',
            'created_at': '2024-01-15',
            'final_amount': 150.50
        }
        """
        try:
            parts = operation_str.split(',')
            
            if len(parts) != 4:
                raise ValueError(f"Formato inválido, esperaba 4 campos, encontró {len(parts)}: {operation_str}")
            
            client_id, store_id, created_at, final_amount_str = parts
            
            return {
                'client_id': client_id,
                'store_id': store_id,
                'created_at': created_at,
                'final_amount': float(final_amount_str)
            }
            
        except Exception as e:
            logger.error(f"Error deserializando operación: {e}")
            raise
    
    def apply_operation(self, operation: dict):
        """
        Aplica una operación al estado en memoria.
        """
        try:
            client_id = operation['client_id']
            store_id = operation['store_id']
            created_at = operation['created_at']
            final_amount = operation['final_amount']
            
            year = created_at[:4]
            year_half = f"{year}-H{self.semester}"
            
            key = (year_half, store_id)
            self.tpv_aggregations_by_client[client_id][key].add_transaction(final_amount)
            
        except Exception as e:
            logger.error(f"Error aplicando operacion: {e}")
            raise

    def _serialize_client_data(self, client_id: str) -> Dict:
        """
        Serializa el estado completo de a un diccionario para checkpoint.
        """
        client_aggregations = self.tpv_aggregations_by_client.get(client_id, {})
        serialized = {}
        
        for (year_half, store_id), aggregation in client_aggregations.items():
            key = f"{year_half}|{store_id}"
            serialized[key] = {
                "total_payment_value": aggregation.total_payment_value,
                "transaction_count": aggregation.transaction_count
            }
        
        return serialized
    
    def _deserialize_client_data(self, client_id: str, data: Dict):
        """
        Reconstruye el estado de desde un diccionario de checkpoint.
        """
        for key, aggregation_data in data.items():
            try:
                year_half, store_id = key.split('|', 1)
            except ValueError:
                logger.warning(f"Key inválido en checkpoint TPV: {key}")
                continue
            
            tuple_key = (year_half, store_id)
            
            if tuple_key not in self.tpv_aggregations_by_client[client_id]:
                self.tpv_aggregations_by_client[client_id][tuple_key] = TPVAggregation()
            
            # Restaurar valores
            aggregation = self.tpv_aggregations_by_client[client_id][tuple_key]
            aggregation.total_payment_value = aggregation_data.get('total_payment_value', 0.0)
            aggregation.transaction_count = aggregation_data.get('transaction_count', 0)
        
        logger.info(f"TPV deserializado para cliente {client_id}: {len(data)} agregaciones")

    def clean_client_data(self, client_id: str):
        """
        Limpia los datos almacenados para un cliente específico.
        """
        if client_id in self.tpv_aggregations_by_client:
            del self.tpv_aggregations_by_client[client_id]
            logger.info(f"Datos TPV limpiados para cliente '{client_id}'")
        else:
            logger.info(f"No hay datos TPV para limpiar para cliente '{client_id}'")