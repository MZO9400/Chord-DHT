import sys

from constants import *


def main():
    ip = "127.0.0.1"
    if len(sys.argv) < 3:
        print("Usage: Client.py <port> <command>")
        sys.exit()
    port = int(sys.argv[1])

    if sys.argv[2] not in ['insert', 'search', 'delete']:
        print("Usage: Client.py <port> <command>")
        sys.exit(1)

    command = sys.argv[2] + "|"

    if sys.argv[2] == 'insert':
        if len(sys.argv) < 5:
            print("Usage: Client.py <port> insert <key> <value>")
            sys.exit()
        command += sys.argv[3] + ":" + sys.argv[4]

    elif sys.argv[2] == 'search':
        if len(sys.argv) < 4:
            print("Usage: Client.py <port> search <key>")
            sys.exit()
        command += sys.argv[3]

    elif sys.argv[2] == 'delete':
        if len(sys.argv) < 4:
            print("Usage: Client.py <port> delete <key>")
            sys.exit()
        command += sys.argv[3]

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((ip, port))
    print("REQUEST:  " + command)
    sock.send(command.encode('utf-8'))
    data = sock.recv(1024)
    data = str(data.decode('utf-8'))
    print("RESPONSE: " + data)
    sock.close()


if __name__ == '__main__':
    main()
