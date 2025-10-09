import logging
import os
from rabbitmq.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
from dtos.dto import TransactionBatchDTO, BatchType
from top_consumers_aggregator import TopCustomersAggregatorNode
from best_selling_aggregator import BestSellingAggregatorNode



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    aggregator_type = os.getenv('AGGREGATOR_TYPE', 'topk')
    
    if aggregator_type in ['best_selling_intermediate', 'best_selling_final']:
        aggregator = BestSellingAggregatorNode()
    elif aggregator_type in ['top_consumers_intermediate', 'top_consumers_final']:
        aggregator = TopCustomersAggregatorNode()
    else:
        raise ValueError(f"AGGREGATOR_TYPE desconocido: {aggregator_type}")
    
    aggregator.start()