import logging
from collections import defaultdict
from typing import Dict, Tuple
from .base_strategy import GroupByStrategy
from configurators.tpv_aggregation import TPVAggregation

logger = logging.getLogger(__name__)


class TPVGroupByStrategy(GroupByStrategy):
    def __init__(self, semester: str,checkpoint_dir: str = None):
        super().__init__(checkpoint_dir=checkpoint_dir)
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
    
    
    def _serialize_client_data(self, client_id: str) -> Dict:
        """
        Serializa el estado de TPV a un diccionario.
        
        Estructura:
        {
            "2024-H1|store1": {
                "total_payment_value": 1500.50,
                "transaction_count": 25
            },
            "2024-H1|store2": {
                "total_payment_value": 3200.75,
                "transaction_count": 45
            }
        }
        
        Nota: Las tuplas (year_half, store_id) se convierten a strings "year_half|store_id"
        porque JSON no soporta tuplas como keys.
        """
        client_aggregations = self.tpv_aggregations_by_client.get(client_id, {})
        serialized = {}
        
        for (year_half, store_id), aggregation in client_aggregations.items():
            # Convertir tupla a string para JSON
            key = f"{year_half}|{store_id}"
            serialized[key] = {
                "total_payment_value": aggregation.total_payment_value,
                "transaction_count": aggregation.transaction_count
            }
        
        return serialized
    
    def _deserialize_client_data(self, client_id: str, data: Dict):
        """
        Reconstruye el estado de TPV desde un diccionario.
        
        Args:
            client_id: ID del cliente
            data: Diccionario con estructura {"year_half|store_id": {"total_payment_value": ..., "transaction_count": ...}}
        """
        for key, aggregation_data in data.items():
            # Parsear key "year_half|store_id" de vuelta a tupla
            try:
                year_half, store_id = key.split('|', 1)
            except ValueError:
                logger.warning(f"Key inválido en checkpoint TPV: {key}")
                continue
            
            tuple_key = (year_half, store_id)
            
            # Crear objeto TPVAggregation si no existe
            if tuple_key not in self.tpv_aggregations_by_client[client_id]:
                self.tpv_aggregations_by_client[client_id][tuple_key] = TPVAggregation()
            
            # Restaurar valores
            aggregation = self.tpv_aggregations_by_client[client_id][tuple_key]
            aggregation.total_payment_value = aggregation_data.get('total_payment_value', 0.0)
            aggregation.transaction_count = aggregation_data.get('transaction_count', 0)
