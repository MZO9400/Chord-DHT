import socket

m = 6  # 2^6 = 64


def send_message(ip, port, message):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((ip, port))
    s.send(message.encode('utf-8'))
    data = s.recv(1024)
    s.close()
    return data.decode('utf-8')
