import time
import socket
import os
import json
import signal
import sys
from configparser import ConfigParser
from common.protocol import Protocol
from rabbitmq.middleware import MessageMiddlewareQueue
from dataclasses import asdict 
from logger import get_logger
from gateway import Gateway
from common.graceful_shutdown import GracefulShutdown

logger = get_logger(__name__)


def initialize_config():
    config = ConfigParser(os.environ)
    config.read("config.ini")

    config_params = {}
    try:
        config_params["port"] = int(os.getenv('SERVER_PORT', config["DEFAULT"]["SERVER_PORT"]))
        config_params["listener_backlog"] = int(os.getenv('SERVER_LISTEN_BACKLOG', config["DEFAULT"]["SERVER_LISTEN_BACKLOG"]))
        config_params["rabbitmq_host"] = os.getenv('RABBITMQ_HOST', config["DEFAULT"]["RABBITMQ_HOST"])
        config_params["output_queue"] = os.getenv('OUTPUT_QUEUE', config["DEFAULT"]["OUTPUT_QUEUE"])
        config_params["reports_exchange"] = os.getenv('REPORTS_EXCHANGE', config.get("DEFAULT", "REPORTS_EXCHANGE", fallback=None))
        config_params["join_exchange"] = os.getenv('JOIN_EXCHANGE', config["DEFAULT"]["JOIN_EXCHANGE"])
        config_params["output_exchange"] = os.getenv('OUTPUT_EXCHANGE', config["DEFAULT"]["OUTPUT_EXCHANGE"])
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params

def main():

    shutdown_handler = GracefulShutdown()
    try:
        config = initialize_config()
        port = config["port"]
        listener_backlog = config["listener_backlog"]
        rabbitmq_host = config["rabbitmq_host"]
        output_queue = config["output_queue"]
        join_exchange = config["join_exchange"]
        reports_exchange = config["reports_exchange"]
        output_exchange = config["output_exchange"]
    
        gateway_instance = Gateway(
                    port, 
                    listener_backlog, 
                    rabbitmq_host, 
                    output_queue, 
                    join_exchange,    
                    output_exchange,
                    reports_exchange,
                    shutdown_handler 
                )
                    
        logger.info("Iniciando Gateway...")
        gateway_instance.start()
        
        logger.info("Gateway terminado exitosamente")
        sys.exit(0) 
        
    except KeyboardInterrupt:
        logger.info("Gateway detenido manualmente (Ctrl+C)")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error fatal en Gateway: {e}")
        sys.exit(1)
        
if __name__ == "__main__":
    main()