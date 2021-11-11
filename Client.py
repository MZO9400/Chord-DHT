import sys

from constants import *


def client(ip, port, command, arguments):
    port = int(port)

    if command == 'insert':
        if len(arguments) != 2:
            raise 'insert: wrong number of arguments'
        command += "|" + arguments[0] + ":" + arguments[1]

    elif command == 'search':
        if len(arguments) != 1:
            raise 'search: wrong number of arguments'
        command += "|" + arguments[0]

    elif command == 'delete':
        if len(arguments) != 1:
            raise 'delete: wrong number of arguments'
        command += "|" + arguments[0]

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((ip, port))
    sock.send(command.encode('utf-8'))
    data = sock.recv(1024)
    data = str(data.decode('utf-8'))
    sock.close()
    return data


if __name__ == '__main__':
    ip = ip
    print("REQUEST", sys.argv)
    port = sys.argv[1]
    command = sys.argv[2]
    arguments = sys.argv[3:]
    print("RESPONSE", client(ip, port, command, arguments))
