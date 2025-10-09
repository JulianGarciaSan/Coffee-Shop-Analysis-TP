"""
Módulo de filtros para el sistema Coffee Shop Analysis.

Este módulo implementa el patrón Strategy para filtrar transacciones
basándose en diferentes criterios como año, hora y cantidad mínima.
"""

"""
Módulo de filtros para el sistema Coffee Shop Analysis.

Este módulo implementa el patrón Strategy para filtrar transacciones
basándose en diferentes criterios como año, hora y cantidad mínima.
"""

# Import strategies
from strategies import (
    FilterStrategy,
    YearFilterStrategy,
    HourFilterStrategy,
    AmountFilterStrategy,
    FilterStrategyFactory
)

# Import configurators  
from configurators import (
    NodeConfigurator,
    YearNodeConfigurator,
    HourNodeConfigurator,
    AmountNodeConfigurator,
    NodeConfiguratorFactory
)

# Import main filter node
from main import FilterNode

__all__ = [
    # Strategies
    'FilterStrategy',
    'YearFilterStrategy', 
    'HourFilterStrategy',
    'AmountFilterStrategy',
    'FilterStrategyFactory',
    # Configurators
    'NodeConfigurator',
    'YearNodeConfigurator',
    'HourNodeConfigurator', 
    'AmountNodeConfigurator',
    'NodeConfiguratorFactory',
    # Main class
    'FilterNode'
]
