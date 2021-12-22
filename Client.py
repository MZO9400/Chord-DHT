import sys

from constants import *


def client(ip, port, command, arguments):
    """
    Creates a socket connection on ip:port and sends command|arguments type of data to the node as binary
    :param ip: IP Address of a node in chord
    :param port: Port of a node in chord
    :param command: One of insert, search, delete
    :param arguments: Dynamic arguments required by one of the above commends
    :return: Response from the Node in chord
    """
    port = int(port)

    if command == 'insert':
        """
        Insert has 2 arguments: one is key, other is value
        """
        if len(arguments) != 2:
            raise 'insert: wrong number of arguments'
        command += "|" + arguments[0] + ":" + arguments[1]

    elif command == 'search':
        """
        Search a given key
        """
        if len(arguments) != 1:
            raise 'search: wrong number of arguments'
        command += "|" + arguments[0]

    elif command == 'delete':
        """
        Delete a given key
        """
        if len(arguments) != 1:
            raise 'delete: wrong number of arguments'
        command += "|" + arguments[0]

    # create a socket connection to ip:port
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((ip, port))
    # send command
    sock.send(command.encode('utf-8'))
    # receive and decode response
    data = sock.recv(1024)
    data = str(data.decode('utf-8'))
    sock.close()
    return data


if __name__ == '__main__':
    """
    Called from commandline, takes commandline parameters in form:
    ./Client.py 9000 insert key value
    """
    ip = ip
    print("REQUEST", sys.argv)
    port = sys.argv[1]
    command = sys.argv[2]
    arguments = sys.argv[3:]
    print("RESPONSE", client(ip, port, command, arguments))
