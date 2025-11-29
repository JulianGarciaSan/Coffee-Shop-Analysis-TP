
from dataclasses import dataclass
from enum import Enum


class DataSourceType(Enum):
    STORES = "stores"
    USERS = "users"
    MENU_ITEMS = "menu_items"
    TPV = "tpv"
    TOP_CUSTOMERS = "top_customers"
    BEST_SELLING = "q2_best_selling"
    MOST_PROFIT = "q2_most_profit"


@dataclass
class ClientProcessingState:
    stores_loaded: bool = False
    users_loaded: bool = False
    menu_items_loaded: bool = False
    top_customers_loaded: bool = False
    best_selling_loaded: bool = False
    most_profit_loaded: bool = False
    groupby_eof_count: int = 0
    expected_groupby_nodes: int = 2
    expected_top_customers_aggregators: int = 2
    top_customers_eof_count: int = 0
    
    q3_results_sent: bool = False
    q4_results_sent: bool = False
    best_selling_sent: bool = False
    most_profit_sent: bool = False
    
    def is_q3_ready(self) -> bool:
        return (self.stores_loaded and 
                self.groupby_eof_count >= self.expected_groupby_nodes and 
                not self.q3_results_sent)
    
    def is_q4_ready(self) -> bool:
        return (self.stores_loaded and 
                self.users_loaded and 
                self.top_customers_loaded and
                self.top_customers_eof_count >= self.expected_top_customers_aggregators and
                not self.q4_results_sent)
    
    def is_best_selling_ready(self) -> bool:
        return (self.menu_items_loaded and 
                self.best_selling_loaded and 
                not self.best_selling_sent)
    
    def is_most_profit_ready(self) -> bool:
        return (self.menu_items_loaded and 
                self.most_profit_loaded and 
                not self.most_profit_sent)
