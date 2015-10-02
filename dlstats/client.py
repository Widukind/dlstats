"""dlstats client 
Usage:
    client.py <command> [<args>...]
    client.py (-h | --help)
    client.py --version

Commands:
	hello         Proof of concept

Options:
    -h --help     Show this screen.
    --version     Show version.
"""
from docopt import docopt
import socket
import configobj
import os
from dlstats import version, configuration

def connect_to_socket():
	socket_path = os.path.normpath(configuration['General']['socket_directory']+'/dlstats.socket')
	client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
	client.connect(socket_path)
	return client

def send_to_socket(string):
    with connect_to_socket() as client:
            client.send(bytes(string, encoding="UTF-8"))
            response = client.recv(512)
            return response.decode()

def list_fetchers():
    print(send_to_socket('list_fetchers'))

def upsert_categories(id):
    print(send_to_socket('upsert_categories '+id))

def upsert_dataset(id):
    print(send_to_socket('upsert_series '+id))

def upsert_a_series(id):
    print(send_to_socket('upsert_series '+id))

def close():
    print(send_to_socket('close'))

commands = {'list_fetchers':list_fetchers,'upsert_catetgories':upsert_categories, 'upsert_a_series':upsert_a_series, 'upsert_dataset':upsert_dataset,'close':close}

if __name__ == '__main__':
    arguments = docopt(__doc__, version=version.version)
    if arguments['<command>'] in commands.keys():
        commands[arguments['<command>']](*arguments['<args>'])
