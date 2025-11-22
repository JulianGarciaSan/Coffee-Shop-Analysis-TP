import logging
from collections import defaultdict
from typing import Dict
from .base_strategy import GroupByStrategy
from .user_purchase_count import UserPurchaseCount

logger = logging.getLogger(__name__)


class TopCustomersGroupByStrategy(GroupByStrategy):
    def __init__(self, input_queue_name: str):
        super().__init__()
        self.input_queue_name = input_queue_name
        self.store_user_purchases_by_client: Dict[str, Dict[str, Dict[str, UserPurchaseCount]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(UserPurchaseCount))
        )
        logger.info(f"TopCustomersGroupByStrategy inicializada para queue {input_queue_name}")

    def save_checkpoint(self, client_id: str, store_id: str, user_id: str, purchases_qty: int):
        """Guarda un checkpoint de un user específico"""
        if not self.logger:
            return
        
        checkpoint_line = f"{client_id}|{store_id}|{user_id}|{purchases_qty}"
        self.logger.write(checkpoint_line)

    def recover_from_checkpoint(self):
        """Recuperar estado completo desde el checkpoint"""
        if not self.logger:
            logger.warning("No hay checkpoint logger configurado")
            return
        
        last_line = self.logger.get_last_line()
        if not last_line:
            logger.info("No hay checkpoint previo, iniciando desde cero")
            return
        
        logger.info("Recuperando estado desde checkpoint...")
        
        with open(self.logger.logger.log_path, 'r') as f:
            line_count = 0
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                try:
                    parts = line.split('|')
                    if len(parts) != 4:
                        continue
                    
                    client_id, store_id, user_id, count = parts
                    
                    if user_id not in self.store_user_purchases_by_client[client_id][store_id]:
                        self.store_user_purchases_by_client[client_id][store_id][user_id] = UserPurchaseCount(user_id)
                    
                    self.store_user_purchases_by_client[client_id][store_id][user_id].purchases_qty = int(count)
                    line_count += 1
                    
                except Exception as e:
                    logger.warning(f"Error recuperando línea de checkpoint: {e}")
        
        logger.info(f"Estado recuperado: {line_count} registros desde checkpoint")

    def process_csv_line(self, csv_line: str, client_id: str = 'default_client'):
        try:
            store_id = self.dto_helper.get_column_value(csv_line, 'store_id')
            user_id = self.dto_helper.get_column_value(csv_line, 'user_id')
            if not store_id or not user_id or user_id.strip() == '':
                return
            
            if user_id not in self.store_user_purchases_by_client[client_id][store_id]:
                self.store_user_purchases_by_client[client_id][store_id][user_id] = UserPurchaseCount(user_id)
            
            self.store_user_purchases_by_client[client_id][store_id][user_id].add_purchase()
            
            # Guardar checkpoint después de cada actualización
            user_purchase = self.store_user_purchases_by_client[client_id][store_id][user_id]
            self.save_checkpoint(client_id, store_id, user_id, user_purchase.purchases_qty)
            self.logger.write_with_timestamp(f"Informo que procese la linea para el cliente {client_id}, tienda {store_id}, usuario {user_id}")
            
        except (ValueError, IndexError) as e:
            logger.warning(f"Error procesando línea: {e}")


    def generate_results_csv_for_client(self, client_id: str) -> str:
        client_data = self.store_user_purchases_by_client.get(client_id, {})
        csv_lines = []
        for store_id in sorted(client_data.keys()):
            store_csv_lines = ["store_id,user_id,purchases_qty"]
            user_purchases = client_data[store_id]
            for user_purchase in user_purchases.values():
                store_csv_lines.append(user_purchase.to_csv_line(store_id))
            csv_lines.extend(store_csv_lines)
        return '\n'.join(csv_lines)