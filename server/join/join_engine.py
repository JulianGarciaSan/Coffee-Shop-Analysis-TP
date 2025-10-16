from typing import Dict, List
import logging

logger = logging.getLogger(__name__)

class JoinEngine:
    @staticmethod
    def join_tpv_stores(tpv_data: List[Dict], stores_data: Dict[str, Dict]) -> List[Dict]:
        """Q3: Join de TPV con stores"""
        joined_data = []
        missing_stores = set()
        
        for tpv_record in tpv_data:
            store_id = tpv_record['store_id']
            
            if store_id in stores_data:
                store_name = stores_data[store_id]['store_name']
            else:
                missing_stores.add(store_id)
                store_name = f"Store_{store_id}"
            
            joined_record = {
                'year_half_created_at': tpv_record['year_half_created_at'],
                'store_id': store_id,
                'store_name': store_name,
                'tpv': tpv_record['total_payment_value']
            }
            joined_data.append(joined_record)
        
        if missing_stores:
            logger.warning(f"Stores no encontrados en Q3: {missing_stores}")
        
        logger.info(f"JOIN Q3 completado: {len(joined_data)} registros")
        return joined_data
    
    @staticmethod
    def join_top_customers(top_customers_data: List[Dict], 
                          stores_data: Dict[str, Dict],
                          users_data: Dict[str, Dict],
                          peer_users_data: Dict[str, Dict] = None) -> List[Dict]:
        joined_data = []
        missing_users = set()
        missing_stores = set()
        
        all_users = {**users_data}
        if peer_users_data:
            all_users.update(peer_users_data)
        
        for customer_record in top_customers_data:
            store_id = customer_record['store_id']
            user_id = customer_record['user_id']
            
            store_name = stores_data.get(store_id, {}).get('store_name', f"Store_{store_id}")
            if store_id not in stores_data:
                missing_stores.add(store_id)
            
            birthdate = all_users.get(user_id, {}).get('birth_date', 'UNKNOWN')
            if user_id not in all_users:
                missing_users.add(user_id)
            
            joined_record = {
                'store_id': store_id,
                'store_name': store_name,
                'birthdate': birthdate
            }
            joined_data.append(joined_record)
        
        if missing_stores:
            logger.warning(f"Stores no encontrados en Q4: {missing_stores}")
        if missing_users:
            logger.warning(f"Users no encontrados en Q4 (primeros 10): {list(missing_users)[:10]}")
        
        logger.info(f"JOIN Q4 completado: {len(joined_data)} registros "
                   f"(users locales: {len(users_data)}, peer users: {len(peer_users_data or {})})")
        return joined_data
    
    @staticmethod
    def join_with_menu_items(data: List[Dict], 
                            menu_items: Dict[str, Dict],
                            value_key: str) -> List[Dict]:
        """Join para Q2 (best_selling y most_profit)"""
        joined_data = []
        missing_items = set()
        
        for record in data:
            item_id = record['item_id']
            
            item_name = menu_items.get(item_id, {}).get('item_name', f"Item_{item_id}")
            if item_id not in menu_items:
                missing_items.add(item_id)
            
            joined_record = {
                'year_month_created_at': record['year_month_created_at'],
                'item_name': item_name,
                value_key: record[value_key]
            }
            joined_data.append(joined_record)
        
        if missing_items:
            logger.warning(f"Items no encontrados: {missing_items}")
        
        logger.info(f"JOIN completado: {len(joined_data)} registros")
        return joined_data