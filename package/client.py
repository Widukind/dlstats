import socket
import configobj
import os
from dlstats import configuration

socket_path = os.path.normpath(configuration['General']['socket_directory']+'/dlstats.socket')
client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
client.connect(socket_path)
client.send(b"hello")
response = client.recv(512)
print(response.decode())
client.close()
