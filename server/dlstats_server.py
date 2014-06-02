import logging
from dlstats import configuration
import os
import glob
import socket
import time

def get_logger(configuration):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    file_handler = logging.FileHandler(os.path.normpath(configuration['logging_directory']) 'dlstats.log')
    file_handler.setLevel(logging.DEBUG)
    frmt = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(frmt)
    logger.addHandler(file_handler)
    return logger

logger = get_logger(configuration)

def event_loop(configuration):
    logger.info('Spawning event loop.')
    socket_path = os.path.normpath(configuration.['socket_directory'] 'dlstats.socket'):
    if os.path.exists( socket_path ):
          os.remove( socket_path )
    server = socket.socket( socket.AF_UNIX, socket.SOCK_STREAM )
    server.bind(socket_path)
    server.listen(5)
    exit_sentinel = False
    while True: 
        connection, address = server.accept()
        while True
            data = connection.recv( 512 )
            if not data or data == 'close':
                break
            else:
                logger.info('Received command: %s', data)
                if data == 'quit':
                    exit_sentinel = True
                elif data in commands:
                    connection.send(commands[data])
                else:
                    connection.send('Unrecognized command: ' data)
        connection.close()
        if exit_sentinel == True:
            break
    server.close()
    logger.info('Quitting event_loop')

if __name__ == "__main__":
    event_loop(configuration)
