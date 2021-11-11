import sys

from Node import Node

ip = "127.0.0.1"


def create(port):
    print("CREATING RING")
    node = Node(ip, int(port))
    node.predecessor = node
    node.successor = node
    node.finger_table.table[0][1] = node
    node.start()


def join(port, port_of_predecessor):
    print("JOINING RING")
    node = Node(ip, int(port))
    node.join(ip, int(port_of_predecessor))
    node.start()


if __name__ == "__main__":
    if len(sys.argv) == 3:
        join(int(sys.argv[1]), int(sys.argv[2]))
    if len(sys.argv) == 2:
        create(int(sys.argv[1]))
