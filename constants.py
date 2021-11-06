import hashlib
import socket

m = 6  # 2^6 = 64


def send_message(ip, port, message):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((ip, port))
    s.send(message.encode('utf-8'))
    data = s.recv(1024)
    s.close()
    return data.decode('utf-8')


def create_hash(message):
    digest = hashlib.sha256(message.encode()).hexdigest()
    digest = int(digest, 16) % pow(2, m)
    return digest


def get_ip_port(string_format):
    ip, port = str(string_format).split(":")
    print(ip, port)
    return ip, int(port)


def get_backward_distance_between_nodes(node2, node1):
    if node2 > node1:
        return node2 - node1
    elif node2 < node1:
        return pow(2, m) - abs(node2 - node1)
    else:
        return 0


def get_forward_distance_between_nodes(node2, node1):
    return pow(2, m) - get_backward_distance_between_nodes(node2, node1)
