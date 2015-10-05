#TODO : Configure the MongoDBJobStore with the configuration file
#TODO : Make the jobs persist when the server is restarted
import logging
import dlstats
import os
import glob
import socket
import time
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.executors.pool import ThreadPoolExecutor

jobstores = {
        'default': MongoDBJobStore(),
}
executors = {
        'default': ThreadPoolExecutor(20),
}
job_defaults = {
        'coalesce': True,
        'max_instances': 1
}

scheduler = BackgroundScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults)

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
    fetchers = [fetcher for fetcher in dlstats.fetchers.__all__ if not fetcher.startswith('_')]
    return ('\n'.join(fetchers))

def upsert_categories(scheduled_time,fetcher,id):
    fetcher = getattr(dlstats.fetchers,fetcher)
    scheduler.add_job(fetcher.upsert_categories,args=id,next_run_time=scheduled_time)

def upsert_dataset(scheduled_time,fetcher,id):
    fetcher = getattr(dlstats.fetchers,fetcher)
    scheduler.add_job(fetcher.upsert_dataset,args=id,next_run_time=scheduled_time)

def upsert_a_series(scheduled_time,fetcher,id):
    fetcher = getattr(dlstats.fetchers,fetcher)
    scheduler.add_job(fetcher.upsert_a_series,args=id,next_run_time=scheduled_time)

def event_loop(configuration):
    logger.info('Spawning event loop.')
    socket_path = os.path.normpath(configuration['General']['socket_directory']+'/dlstats.socket')
    if os.path.exists( socket_path ):
          os.remove( socket_path )
    server = socket.socket( socket.AF_UNIX, socket.SOCK_STREAM )
    try:
        server.bind(socket_path)
    except FileNotFoundError:
        os.makedirs(configuration['General']['socket_directory'])
    server.listen(5)
    commands = {'list_fetchers':list_fetchers,
                'upsert_categories': upsert_categories,
                'upsert_dataset': upsert_dataset}
    exit_sentinel = False
    while True: 
        connection, address = server.accept()
        while True:
            data = connection.recv( 512 ).decode()
            data_ = data.split(' ')
            command = data_[0]
            if len(data_) > 1:
                args = data_[1:end]
            else:
                args = None
            if not command:
                break
            else:
                logger.info('Received command: %s', data)
                if command == 'close':
                    connection.send('Closing dlstats server'.encode())
                    exit_sentinel = True
                elif command in commands.keys():
                    if args is not None:
                        connection.send(commands[command](*args).encode())
                    else:
                        connection.send(commands[command]().encode())
                else:
                    connection.send('Unrecognized command: '+data)
        connection.close()
        if exit_sentinel == True:
            break
    server.close()
    logger.info('Quitting event_loop')

def main():
    event_loop(dlstats.configuration)    

if __name__ == "__main__":
    main()
