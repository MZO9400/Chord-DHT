import socket
import sys


def main():
    ip = "127.0.0.1"
    if len(sys.argv) < 2:
        print("Usage: Client.py <port>")
        sys.exit()
    port = int(sys.argv[1])

    while True:
        print("1. TO ENTER")
        print("2. TO SHOW")
        print("3. TO DELETE")
        print("ELSE EXIT")
        choice = input()
        message = None

        if choice == '1':
            key = input("ENTER THE KEY: ")
            val = input("ENTER THE VALUE: ")
            message = "insert|" + str(key) + ":" + str(val)
        elif choice == '2':
            key = input("ENTER THE KEY: ")
            message = "search|" + str(key)
        elif choice == '3':
            key = input("ENTER THE KEY: ")
            message = "delete|" + str(key)
        else:
            print("Exiting Client")
            exit(0)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((ip, port))
        print("REQUEST:  " + message)
        sock.send(message.encode('utf-8'))
        data = sock.recv(1024)
        data = str(data.decode('utf-8'))
        print("RESPONSE: " + data)
        sock.close()


if __name__ == '__main__':
    main()
