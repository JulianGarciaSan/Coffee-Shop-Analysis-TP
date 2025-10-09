import logging
from typing import Optional
from .base_strategy import FilterStrategy
from .year_filter_strategy import YearFilterStrategy
from .hour_filter_strategy import HourFilterStrategy
from .amount_filter_strategy import AmountFilterStrategy

logger = logging.getLogger(__name__)

class FilterStrategyFactory:
    @staticmethod
    def create_strategy(filter_mode: str, **config) -> Optional[FilterStrategy]:
        if filter_mode == 'year':
            filter_years = config.get('filter_years')
            if not filter_years:
                raise ValueError("filter_years is required for year filter strategy")
            
            if isinstance(filter_years, str):
                filter_years = filter_years.split(',')
                
            logger.info(f"Creating YearFilterStrategy with years: {filter_years}")
            return YearFilterStrategy(filter_years)
            
        elif filter_mode == 'hour':
            filter_hours = config.get('filter_hours')
            if not filter_hours:
                raise ValueError("filter_hours is required for hour filter strategy")
                
            logger.info(f"Creating HourFilterStrategy with hours: {filter_hours}")
            return HourFilterStrategy(filter_hours)
            
        elif filter_mode == 'amount':
            min_amount = config.get('min_amount')
            if min_amount is None:
                raise ValueError("min_amount is required for amount filter strategy")
                
            try:
                min_amount = float(min_amount)
            except (ValueError, TypeError):
                raise ValueError(f"min_amount must be a valid number, got: {min_amount}")
                
            logger.info(f"Creating AmountFilterStrategy with min_amount: ${min_amount}")
            return AmountFilterStrategy(min_amount)
            
        else:
            logger.error(f"Unknown filter mode: {filter_mode}")
            return None
    
    @staticmethod
    def get_available_strategies() -> list:
        return ['year', 'hour', 'amount']
    
    @staticmethod
    def get_strategy_requirements(filter_mode: str) -> dict:
        requirements = {
            'year': {
                'filter_years': 'Lista de años permitidos (ej: "2024,2025" o ["2024", "2025"])'
            },
            'hour': {
                'filter_hours': 'Rango horario en formato "HH:MM-HH:MM" (ej: "06:00-22:59")'
            },
            'amount': {
                'min_amount': 'Monto mínimo requerido (ej: 75.0)'
            }
        }
        
        return requirements.get(filter_mode, {})
