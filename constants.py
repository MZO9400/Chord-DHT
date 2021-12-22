import hashlib
import socket

# M is the maximum number of nodes in the chord
m = 6  # 2^6 = 64

# All nodes are local to the system, make dynamic changes if deploying over a distributed network
ip = "localhost"


def send_message(ip, port, message):
    """
    Send message function that connects to a socket and sends message to it
    :param ip: ip of node
    :param port: port of node
    :param message: string message to send
    :return: response from the node
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((ip, port))
    s.send(message.encode('utf-8'))
    data = s.recv(1024)
    s.close()
    return data.decode('utf-8')


def create_hash(message):
    """
    SHA256 encrypt a message, convert the digest to hexadecimal values, convert into base 10, mod from 2^m
    :param message: String message
    :return: unique key of the message from 0-2^m namespace
    """
    digest = hashlib.sha256(message.encode()).hexdigest()
    digest = int(digest, 16) % pow(2, m)
    return digest


def get_ip_port(string_format):
    """
    Split IP:Port string to ip and port
    :param string_format: IP:Port format string
    :return: ip, port number
    """
    ip, port = str(string_format).split(":")
    return ip, int(port)


def get_backward_distance_between_nodes(node2, node1):
    """
    Get backward distance from one node to another
    :param node2: key of node 2
    :param node1: key of node 1
    :return: numeric distance between nodes
    """
    if node2 > node1:
        # i.e. 52 - 2 = 50 distance
        return node2 - node1
    elif node2 < node1:
        # i.e. 64 - 50 = 14 distance
        return pow(2, m) - abs(node2 - node1)
    else:
        # same node, 0 distance
        return 0


def get_forward_distance_between_nodes(node2, node1):
    """
    MAX - backward distance = forward distance
    :param node2: key of node2
    :param node1: key of node1
    :return: numeric forward distance
    """
    return pow(2, m) - get_backward_distance_between_nodes(node2, node1)
