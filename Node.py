import random
import threading
import time

from DataStructures import *
from constants import *


class Node:
    """
    Node class used in the chord.
    """

    def __init__(self, ip, port):
        """
        Initialize the node at IP:Port
        :param ip: ip of the node
        :param port: port of the node
        """
        self.ip = ip
        self.port = int(port)
        self.address = Address(ip, port)
        self.id = create_hash(str(self.address))  # create a hashed id of a node from its IP

        # successor and predecessor of a node are empty when it is initialized
        self.predecessor = None
        self.successor = None

        self.finger_table = FingerTable(self.id)
        self.db = Database()

        self.thread_for_fix_finger = None
        self.thread_for_stabilize = None

    def process_requests(self, message):
        """
        Process request coming in from a client or a node
        :param message: command in format: command|arguments
        :return: Response
        """
        operation = message.split("|")[0]  # take command i.e. left side of pipe
        args = []
        if len(message.split("|")) > 1:
            args = message.split("|")[1:]
        result = "Done"
        if operation == 'insert_server':  # if command is insert_server, insert key:value into self
            data = message.split('|')[1].split(":")
            key = data[0]
            value = data[1]
            self.db.insert(key, value)
            result = 'Inserted'

        if operation == "delete_server":  # if command is delete_server, delete key into self
            data = message.split('|')[1]
            self.db.data.pop(data)
            result = 'Deleted'

        if operation == "search_server":  # if command is search_server, search key to fetch its value
            data = message.split('|')[1]
            if data in self.db.data:
                return self.db.data[data]
            else:
                return "NOT FOUND"

        if operation == "send_keys":  # send key value pairs to the joining node
            id_of_joining_node = int(args[0])
            result = self.send_keys(id_of_joining_node)

        if operation == "insert":  # Insert key-value into the chord network
            data = message.split('|')[1].split(":")
            print(message, data)
            key = data[0]
            value = data[1]
            result = self.insert_key(key, value)

        if operation == "delete":  # Delete key from chord network
            data = message.split('|')[1]
            result = self.delete_key(data)

        if operation == 'search':  # Search key in chord network
            data = message.split('|')[1]
            result = self.search_key(data)

        if operation == "join_request":  # Process a join request from another node
            result = self.join_request_from_other_node(int(args[0]))

        if operation == "find_predecessor":  # Find predecessor of a given node
            result = self.find_predecessor(int(args[0]))

        if operation == "find_successor":  # Find successor of a given node
            result = self.find_successor(int(args[0]))

        if operation == "get_successor":  # Fetch the found successor of self
            result = self.get_successor()

        if operation == "get_predecessor":  # Fetch the found predecessor of self
            result = self.get_predecessor()

        if operation == "get_id":  # Get ID of a node at IP:Port
            result = self.id

        if operation == "notify":  # Notify a node about your id, ip, and port
            ip, port = args[1].split(":")
            self.notify(int(args[0]), ip, port)

        return str(result)

    def serve_requests(self, conn):
        """
        Serve an incoming connection and process its request using process_request function
        :param conn: Socket connection
        :return: None
        """
        with conn:
            data = conn.recv(1024)  # receive 1KB from the client
            data = str(data.decode('utf-8'))
            data = data.strip('\n')
            data = self.process_requests(data)  # process the request
            data = bytes(str(data), 'utf-8')
            conn.sendall(data)  # send the response the client in binary

    def start(self):
        """
        Initializer function that is called when a node is created
        Sets up node
        :return:
        """
        self.thread_for_stabilize = threading.Thread(target=self.stabilize)
        self.thread_for_stabilize.start()  # start the thread to fix successor and predecessor
        self.thread_for_fix_finger = threading.Thread(target=self.fix_fingers)
        self.thread_for_fix_finger.start()  # Fix random finger table connections
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.address.ip, self.address.port))
            s.listen()
            while True:
                conn, addr = s.accept()  # accept a connection
                t = threading.Thread(target=self.serve_requests, args=[conn])
                t.start()  # run the thread to serve an incoming request

    def insert_key(self, key, value):
        """
        Insert key function to insert the key into the DHT
        :param key: un-hashed key
        :param value: value
        :return: Response
        """
        id_of_key = create_hash(str(key))  # hash the key
        successor = self.find_successor(id_of_key)  # find the successor of the hashed key
        ip, port = get_ip_port(successor)  # get the IP:Port of the successor
        send_message(ip, port, "insert_server|" + str(key) + ":" + str(value))  # form the command
        return "Inserted at node id " + str(Node(ip, port).id) + " key was " + str(key) + " key hash was " + str(
            id_of_key)

    def delete_key(self, key):
        """
        Delete the key from chord, same operation as insert_key
        :param key: Un-hashed key
        :return: Response
        """
        id_of_key = create_hash(str(key))
        successor = self.find_successor(id_of_key)
        ip, port = get_ip_port(successor)
        send_message(ip, port, "delete_server|" + str(key))
        return "deleted at node id " + str(Node(ip, port).id) + " key was " + str(key) + " key hash was " + str(
            id_of_key)

    def search_key(self, key):
        """
        Search a key from chord, same operation as insert_key
        :param key: Un-hashed key
        :return: Response
        """
        id_of_key = create_hash(str(key))
        successor = self.find_successor(id_of_key)
        ip, port = get_ip_port(successor)
        data = send_message(ip, port, "search_server|" + str(key))
        return data

    def join_request_from_other_node(self, node_id):
        """
        Fetch the successor of joining node to send back
        :param node_id:
        :return: IP:Port of the joining node's successor
        """
        return self.find_successor(node_id)

    def join(self, node_ip, node_port):
        """
        Send join request to ip:port
        :param node_ip: ip to join
        :param node_port: port to join to
        :return:
        """
        data = 'join_request|' + str(self.id)  # form the request
        successor = send_message(node_ip, node_port, data)
        ip, port = get_ip_port(successor)
        self.successor = Node(ip, port)
        self.finger_table.table[0][1] = self.successor  # Put the successor into the finger table
        self.predecessor = None

        if self.successor.id != self.id:  # send keys to the new joining node
            data = send_message(self.successor.ip, self.successor.port, "send_keys|" + str(self.id))
            for key_value in data.split(':'):
                if len(key_value) > 1:
                    self.db.data[key_value.split('|')[0]] = key_value.split('|')[1]

    def find_predecessor(self, search_id):
        """
        Find predecessor of a node id
        :param search_id: Key of node search_id
        :return: IP address of predecessor
        """
        if search_id == self.id:
            return str(self.address)
        if self.predecessor is not None and self.successor.id == self.id:  # if pred is none
            return self.address.__str__()  # return ip of self
        if get_forward_distance_between_nodes(self.id, self.successor.id) > \
                get_forward_distance_between_nodes(self.id, search_id):
            # if distance between successor is less than current
            return self.address.__str__()  # return ip of self
        else:
            new_node_hop = self.closest_preceding_node(search_id)
            if new_node_hop is None:
                return "None"
            ip, port = get_ip_port(str(new_node_hop.address.__str__()))  # get ip address of next hop
            if ip == self.ip and port == self.port:
                return self.address.__str__()
            data = send_message(ip, port, "find_predecessor|" + str(search_id))
            return data

    def find_successor(self, search_id):
        """
        Fetch the successor of a node
        :param search_id: node id
        :return:
        """
        if search_id == self.id:
            #  if key is self
            return str(self.address)
        predecessor = self.find_predecessor(search_id)
        if predecessor == "None":
            return "None"
        ip, port = get_ip_port(predecessor)
        data = send_message(ip, port, "get_successor")
        return data

    def closest_preceding_node(self, search_id):
        """
        get closest preceding node using count of keys
        Simple min algorithm to start at 0 and replace node if a smaller node is found
        :param search_id: node to find the preceding node from
        :return: IP Address
        """
        closest_node = None
        min_distance = pow(2, m) + 1
        for i in list(reversed(range(m))):
            if self.finger_table.table[i][1] is not None and get_forward_distance_between_nodes(
                    self.finger_table.table[i][1].id, search_id) < min_distance:
                closest_node = self.finger_table.table[i][1]
                min_distance = get_forward_distance_between_nodes(self.finger_table.table[i][1].id, search_id)

        return closest_node

    def send_keys(self, id_of_joining_node):
        """
        Send keys and values to the joining node for load balancing
        :param id_of_joining_node: node id
        :return: Keys
        """
        data = ""
        keys_to_be_removed = []
        for keys in self.db.data:
            key_id = create_hash(str(keys))
            if get_forward_distance_between_nodes(key_id, id_of_joining_node) < \
                    get_forward_distance_between_nodes(key_id, self.id):
                # get all keys where key is less than  node's key
                data += str(keys) + "|" + str(self.db.data[keys]) + ":"
                keys_to_be_removed.append(keys)
        for keys in keys_to_be_removed:
            self.db.data.pop(keys)
        print(data)
        return data

    def stabilize(self):
        """
        Stabilize the node when a new node joins and successor/predecessor changes
        Update in finger table as well as successor and predecessor
        :return: None
        """
        while True: # run code every 10 seconds
            if self.successor is None:
                time.sleep(10)
                continue
            data = "get_predecessor"

            if self.successor.ip == self.ip and self.successor.port == self.port:
                time.sleep(10)
            result = send_message(self.successor.ip, self.successor.port, data)
            if result == "None" or len(result) == 0:
                send_message(self.successor.ip, self.successor.port,
                             "notify|" + str(self.id) + "|" + self.address.__str__())
                # notify the successor/predecessor for ip port change
                continue

            ip, port = get_ip_port(result)
            result = int(send_message(ip, port, "get_id"))
            if get_backward_distance_between_nodes(self.id, result) > \
                    get_backward_distance_between_nodes(self.id, self.successor.id):
                self.successor = Node(ip, port)
                self.finger_table.table[0][1] = self.successor
            send_message(self.successor.ip, self.successor.port,
                         "notify|" + str(self.id) + "|" + self.address.__str__())
            print("ID: ", self.id)
            if self.successor is not None:
                print("Successor ID: ", self.successor.id)
            if self.predecessor is not None:
                print("predecessor ID: ", self.predecessor.id)
            self.finger_table.print()
            print(str(self.db.data))
            time.sleep(10)

    def notify(self, node_id, node_ip, node_port):
        """
        Notify the port and set predecessor / successor to the response
        :param node_id: id to notify
        :param node_ip: ip to notify
        :param node_port: port to notify
        :return: None
        """
        if self.predecessor is not None:
            if get_backward_distance_between_nodes(self.id, node_id) < \
                    get_backward_distance_between_nodes(self.id, self.predecessor.id):
                self.predecessor = Node(node_ip, int(node_port))
                return
        if self.predecessor is None or self.predecessor == "None" or \
                (self.predecessor.id < node_id < self.id) or \
                (self.id == self.predecessor.id and node_id != self.id):
            self.predecessor = Node(node_ip, int(node_port))
            if self.id == self.successor.id:
                self.successor = Node(node_ip, int(node_port))
                self.finger_table.table[0][1] = self.successor

    def fix_fingers(self):
        """
        Fix random finger every 10 seconds
        :return: None
        """
        while True:
            random_index = random.randint(1, m - 1)
            finger = self.finger_table.table[random_index][0]
            data = self.find_successor(finger)
            if data == "None":
                time.sleep(10)
                continue
            ip, port = get_ip_port(data)
            self.finger_table.table[random_index][1] = Node(ip, port)
            time.sleep(10)

    def get_successor(self):
        """
        Get successor of self
        :return: Node Address
        """
        return "None" if self.successor is None else self.successor.address.__str__()

    def get_predecessor(self):
        """
        Get predecessor of self
        :return: Node Address
        """
        return "None" if self.predecessor is None else self.predecessor.address.__str__()
