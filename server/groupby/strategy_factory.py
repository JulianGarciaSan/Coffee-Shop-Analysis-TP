
from typing import List
from base_strategy import GroupByStrategy
from tpv_groupby_strategy import TPVGroupByStrategy
from top_customers_groupby_strategy import TopCustomersGroupByStrategy
from best_selling_groupby_strategy import BestSellingGroupByStrategy


class GroupByStrategyFactory:
    """Factory para crear estrategias de agrupación."""
    
    _strategies = {
        'tpv': TPVGroupByStrategy,
        'top_customers': TopCustomersGroupByStrategy,
        'best_selling': BestSellingGroupByStrategy
    }
    
    @staticmethod
    def create_strategy(groupby_mode: str, **config) -> GroupByStrategy:
        strategy_class = GroupByStrategyFactory._strategies.get(groupby_mode)
        
        if strategy_class is None:
            raise ValueError(f"Unknown groupby mode: {groupby_mode}")
        
        if groupby_mode == 'tpv':
            semester = config.get('semester')
            if semester not in ['1', '2']:
                raise ValueError("semester must be '1' or '2' for TPV")
            return strategy_class(semester)
        
        elif groupby_mode == 'top_customers':
            input_queue_name = config.get('input_queue_name')
            if not input_queue_name:
                raise ValueError("input_queue_name is required for top_customers")
            return strategy_class(input_queue_name)
        
        elif groupby_mode == 'best_selling':
            input_queue_name = config.get('input_queue_name')
            if not input_queue_name:
                raise ValueError("input_queue_name is required for best_selling")
            return strategy_class(input_queue_name)
        
        return strategy_class(**config)
    
    @staticmethod
    def register_strategy(groupby_mode: str, strategy_class: type):
        """Registra una nueva estrategia."""
        GroupByStrategyFactory._strategies[groupby_mode] = strategy_class
    
    @staticmethod
    def get_available_strategies() -> List[str]:
        """Retorna lista de estrategias disponibles."""
        return list(GroupByStrategyFactory._strategies.keys())