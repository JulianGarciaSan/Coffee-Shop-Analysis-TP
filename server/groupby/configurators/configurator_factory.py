from .base_configurators import GroupByConfigurator
from .tpv_configurator import TPVConfigurator
#from top_customers_configurator import TopCustomersConfigurator
#from best_selling_configurator import BestSellingConfigurator


class GroupByConfiguratorFactory:    
    _configurators = {
        'tpv': TPVConfigurator,
        #'top_customers': TopCustomersConfigurator,
        #'best_selling': BestSellingConfigurator
    }
    
    @staticmethod
    def create_configurator(groupby_mode: str, rabbitmq_host: str, output_exchange: str) -> GroupByConfigurator:
        configurator_class = GroupByConfiguratorFactory._configurators.get(groupby_mode)
        
        if configurator_class is None:
            raise ValueError(f"Unknown groupby mode: {groupby_mode}")
        
        return configurator_class(rabbitmq_host, output_exchange)

