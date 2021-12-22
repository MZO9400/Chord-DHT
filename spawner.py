import sys

from Node import Node
from constants import ip


def create(port):
    """
    Create a DHT on port
    :param port: port of a node
    :return: None
    """
    print("CREATING RING")
    node = Node(ip, int(port))
    node.predecessor = node
    node.successor = node
    node.finger_table.table[0][1] = node
    node.start()


def join(port, port_of_predecessor):
    """
    Join a DHT
    :param port: new node's port
    :param port_of_predecessor: port from DHT
    :return: None
    """
    print("JOINING RING")
    node = Node(ip, int(port))
    node.join(ip, int(port_of_predecessor))
    node.start()


if __name__ == "__main__":
    """
    Commandline API to interpret arguments and create/join networks
    """
    if len(sys.argv) == 3:
        join(int(sys.argv[1]), int(sys.argv[2]))
    if len(sys.argv) == 2:
        create(int(sys.argv[1]))
