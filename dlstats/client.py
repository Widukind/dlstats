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
from dlstats import version

def connect_to_socket():
	socket_path = os.path.normpath(configuration['General']['socket_directory']+'/dlstats.socket')
	client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
	client.connect(socket_path)
	return client

def hello():
	with connect_to_socket() as client:
		client.send(b"hello")
		response = client.recv(512)
		return response.decode()

commands = {'hello':hello}

if __name__ == '__main__':
    arguments = docopt(__doc__, version=version.version)
    if arguments['command'] in commands.keys():
        commands[arguments['command']](**arguments['args'])
