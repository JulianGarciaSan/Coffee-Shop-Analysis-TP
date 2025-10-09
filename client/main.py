#!/usr/bin/env python3

from configparser import ConfigParser
import logging
import os
import signal
import sys

from client import Client

def initialize_config():
    """ Parse env variables or config file to find program config params

    Function that search and parse program configuration parameters in the
    program environment variables first and the in a config file. 
    If at least one of the config parameters is not found a KeyError exception 
    is thrown. If a parameter could not be parsed, a ValueError is thrown. 
    If parsing succeeded, the function returns a ConfigParser object 
    with config parameters
    """

    config = ConfigParser(os.environ)
    # If config.ini does not exists original config object is not modified
    config.read("config.ini")

    config_params = {}
    try:
        config_params["port"] = int(os.getenv('PORT', config["DEFAULT"]["PORT"]))
        config_params["max_batch_size"] = int(os.getenv('MAX_BATCH_SIZE', config["DEFAULT"]["MAX_BATCH_SIZE"]))
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params

def initialize_log(logging_level):
    """
    Python custom logging initialization

    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging_level,
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    
    
def main():
    try:
        config_params = initialize_config()
        logging_level = config_params["logging_level"]
        server_port = config_params["port"]
        max_batch_size = config_params["max_batch_size"]

        initialize_log(logging_level)

        #signal.signal(signal.SIGTERM, signal_handler) GRACEFUL SHUTDOWN
        client = Client(server_port, max_batch_size)
        client.run()
        sys.exit(0) 
        
    except KeyboardInterrupt:
        logging.info("Client detenido manualmente (Ctrl+C)")
        sys.exit(0)
    except Exception as e:
        logging.error(f"Error fatal en Client: {e}")
        sys.exit(1) 
        
if __name__ == "__main__":
    main()