from .base_configurator import NodeConfigurator
from .year_node_configurator import YearNodeConfigurator
from .hour_node_configurator import HourNodeConfigurator
from .amount_node_configurator import AmountNodeConfigurator


class NodeConfiguratorFactory:
    _configurators = {
        'year': YearNodeConfigurator,
        'hour': HourNodeConfigurator,
        'amount': AmountNodeConfigurator
    }
    
    @staticmethod
    def create_configurator(filter_mode: str, rabbitmq_host: str) -> NodeConfigurator:
        configurator_class = NodeConfiguratorFactory._configurators.get(filter_mode)
        
        if configurator_class is None:
            raise ValueError(f"Unknown filter mode: {filter_mode}")
        
        return configurator_class(rabbitmq_host)