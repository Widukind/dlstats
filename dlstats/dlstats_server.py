import logging
import dlstats
import os
import glob
import socket
import time

def get_logger(configuration):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    file_handler = logging.FileHandler(os.path.normpath(configuration['General']['logging_directory'])+'/dlstats.log')
    file_handler.setLevel(logging.DEBUG)
    frmt = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(frmt)
    logger.addHandler(file_handler)
    return logger

logger = get_logger(dlstats.configuration)

def list_fetchers():
    fetchers = [fetcher for fetcher in dir(dlstats.fetchers) if not fetcher.startswith('_')]
    return (', '.join(fetchers))

def event_loop(configuration):
    logger.info('Spawning event loop.')
    socket_path = os.path.normpath(configuration['General']['socket_directory']+'/dlstats.socket')
    if os.path.exists( socket_path ):
          os.remove( socket_path )
    server = socket.socket( socket.AF_UNIX, socket.SOCK_STREAM )
    server.bind(socket_path)
    server.listen(5)
    commands = {'list_fetchers':list_fetchers}
    exit_sentinel = False
    while True: 
        connection, address = server.accept()
        while True:
            data = connection.recv( 512 ).decode()
            if not data or data == 'close':
                break
            else:
                logger.info('Received command: %s', data)
                if data == 'quit':
                    exit_sentinel = True
                elif data in commands.keys():
                    connection.send(commands[data]().encode())
                else:
                    connection.send('Unrecognized command: '+data)
        connection.close()
        if exit_sentinel == True:
            break
    server.close()
    logger.info('Quitting event_loop')

if __name__ == "__main__":
    event_loop(dlstats.configuration)
