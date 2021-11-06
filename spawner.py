import sys

from Node import Node

ip = "127.0.0.1"

if len(sys.argv) == 3:
    print("JOINING RING")
    node = Node(ip, int(sys.argv[1]))
    node.join(ip, int(sys.argv[2]))
    node.start()

if len(sys.argv) == 2:
    print("CREATING RING")
    node = Node(ip, int(sys.argv[1]))
    node.predecessor = node
    node.successor = node
    node.finger_table.table[0][1] = node
    node.start()
