import random
import threading
import time

from DataStructures import *
from constants import *


class Node:
    def __init__(self, ip, port):
        self.thread_for_fix_finger = None
        self.thread_for_stabilize = None
        self.ip = ip
        self.port = int(port)
        self.address = Address(ip, port)
        self.id = create_hash(str(self.address))
        self.predecessor = None
        self.successor = None
        self.finger_table = FingerTable(self.id)
        self.db = Database()

    def process_requests(self, message):
        operation = message.split("|")[0]
        args = []
        if len(message.split("|")) > 1:
            args = message.split("|")[1:]
        result = "Done"
        if operation == 'insert_server':
            data = message.split('|')[1].split(":")
            key = data[0]
            value = data[1]
            self.db.insert(key, value)
            result = 'Inserted'

        if operation == "delete_server":
            data = message.split('|')[1]
            self.db.data.pop(data)
            result = 'Deleted'

        if operation == "search_server":
            data = message.split('|')[1]
            if data in self.db.data:
                return self.db.data[data]
            else:
                return "NOT FOUND"

        if operation == "send_keys":
            id_of_joining_node = int(args[0])
            result = self.send_keys(id_of_joining_node)

        if operation == "insert":
            data = message.split('|')[1].split(":")
            print(message, data)
            key = data[0]
            value = data[1]
            result = self.insert_key(key, value)

        if operation == "delete":
            data = message.split('|')[1]
            result = self.delete_key(data)

        if operation == 'search':
            data = message.split('|')[1]
            result = self.search_key(data)

        if operation == "join_request":
            result = self.join_request_from_other_node(int(args[0]))

        if operation == "find_predecessor":
            result = self.find_predecessor(int(args[0]))

        if operation == "find_successor":
            result = self.find_successor(int(args[0]))

        if operation == "get_successor":
            result = self.get_successor()

        if operation == "get_predecessor":
            result = self.get_predecessor()

        if operation == "get_id":
            result = self.id

        if operation == "notify":
            ip, port = args[1].split(":")
            self.notify(int(args[0]), ip, port)

        return str(result)

    def serve_requests(self, conn):
        with conn:
            data = conn.recv(1024)
            data = str(data.decode('utf-8'))
            data = data.strip('\n')
            data = self.process_requests(data)
            data = bytes(str(data), 'utf-8')
            conn.sendall(data)

    def start(self):
        self.thread_for_stabilize = threading.Thread(target=self.stabilize)
        self.thread_for_stabilize.start()
        self.thread_for_fix_finger = threading.Thread(target=self.fix_fingers)
        self.thread_for_fix_finger.start()
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.address.ip, self.address.port))
            s.listen()
            while True:
                conn, addr = s.accept()
                t = threading.Thread(target=self.serve_requests, args=[conn])
                t.start()

    def insert_key(self, key, value):
        id_of_key = create_hash(str(key))
        successor = self.find_successor(id_of_key)
        ip, port = get_ip_port(successor)
        send_message(ip, port, "insert_server|" + str(key) + ":" + str(value))
        return "Inserted at node id " + str(Node(ip, port).id) + " key was " + str(key) + " key hash was " + str(
            id_of_key)

    def delete_key(self, key):
        id_of_key = create_hash(str(key))
        successor = self.find_successor(id_of_key)
        ip, port = get_ip_port(successor)
        send_message(ip, port, "delete_server|" + str(key))
        return "deleted at node id " + str(Node(ip, port).id) + " key was " + str(key) + " key hash was " + str(
            id_of_key)

    def search_key(self, key):
        id_of_key = create_hash(str(key))
        successor = self.find_successor(id_of_key)
        ip, port = get_ip_port(successor)
        data = send_message(ip, port, "search_server|" + str(key))
        return data

    def join_request_from_other_node(self, node_id):
        return self.find_successor(node_id)

    def join(self, node_ip, node_port):
        data = 'join_request|' + str(self.id)
        successor = send_message(node_ip, node_port, data)
        ip, port = get_ip_port(successor)
        self.successor = Node(ip, port)
        self.finger_table.table[0][1] = self.successor
        self.predecessor = None

        if self.successor.id != self.id:
            data = send_message(self.successor.ip, self.successor.port, "send_keys|" + str(self.id))
            for key_value in data.split(':'):
                if len(key_value) > 1:
                    self.db.data[key_value.split('|')[0]] = key_value.split('|')[1]

    def find_predecessor(self, search_id):
        if search_id == self.id:
            return str(self.address)
        if self.predecessor is not None and self.successor.id == self.id:
            return self.address.__str__()
        if get_forward_distance_between_nodes(self.id, self.successor.id) > \
                get_forward_distance_between_nodes(self.id, search_id):
            return self.address.__str__()
        else:
            new_node_hop = self.closest_preceding_node(search_id)
            if new_node_hop is None:
                return "None"
            ip, port = get_ip_port(str(new_node_hop.address.__str__()))
            if ip == self.ip and port == self.port:
                return self.address.__str__()
            data = send_message(ip, port, "find_predecessor|" + str(search_id))
            return data

    def find_successor(self, search_id):
        if search_id == self.id:
            return str(self.address)
        predecessor = self.find_predecessor(search_id)
        if predecessor == "None":
            return "None"
        ip, port = get_ip_port(predecessor)
        data = send_message(ip, port, "get_successor")
        return data

    def closest_preceding_node(self, search_id):
        closest_node = None
        min_distance = pow(2, m) + 1
        for i in list(reversed(range(m))):
            if self.finger_table.table[i][1] is not None and get_forward_distance_between_nodes(
                    self.finger_table.table[i][1].id, search_id) < min_distance:
                closest_node = self.finger_table.table[i][1]
                min_distance = get_forward_distance_between_nodes(self.finger_table.table[i][1].id, search_id)

        return closest_node

    def send_keys(self, id_of_joining_node):
        data = ""
        keys_to_be_removed = []
        for keys in self.db.data:
            key_id = create_hash(str(keys))
            if get_forward_distance_between_nodes(key_id, id_of_joining_node) < \
                    get_forward_distance_between_nodes(key_id, self.id):
                data += str(keys) + "|" + str(self.db.data[keys]) + ":"
                keys_to_be_removed.append(keys)
        for keys in keys_to_be_removed:
            self.db.data.pop(keys)
        print(data)
        return data

    def stabilize(self):
        while True:
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
                continue

            ip, port = get_ip_port(result)
            result = int(send_message(ip, port, "get_id"))
            if get_backward_distance_between_nodes(self.id, result) > \
                    get_backward_distance_between_nodes(self.id, self.successor.id):
                self.successor = Node(ip, port)
                self.finger_table.table[0][1] = self.successor
            send_message(self.successor.ip, self.successor.port,
                         "notify|" + str(self.id) + "|" + self.address.__str__())
            print("===============================================")
            print("STABILIZING")
            print("===============================================")
            print("ID: ", self.id)
            if self.successor is not None:
                print("Successor ID: ", self.successor.id)
            if self.predecessor is not None:
                print("predecessor ID: ", self.predecessor.id)
            print("===============================================")
            print("=============== FINGER TABLE ==================")
            self.finger_table.print()
            print("===============================================")
            print("DATA STORE")
            print("===============================================")
            print(str(self.db.data))
            print("===============================================")
            print("+++++++++++++++ END +++++++++++++++++++++++++++")
            time.sleep(10)

    def notify(self, node_id, node_ip, node_port):
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
        return "None" if self.successor is None else self.successor.address.__str__()

    def get_predecessor(self):
        return "None" if self.predecessor is None else self.predecessor.address.__str__()
