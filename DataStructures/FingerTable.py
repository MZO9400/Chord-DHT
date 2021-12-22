from constants import *


class FingerTable:
    """
    A finger table to save information of neighbouring nodes as well as ip's of  all the nodes in the chord
    """
    def __init__(self, id):
        """
        Initializer for a fingertable
        :param id: Given a key, generates a finger table for the node
        """
        self.table = []
        for i in range(m):
            x = pow(2, i)
            entry = (id + x) % pow(2, m)
            node = None
            self.table.append([entry, node])

    def print(self):
        """
        Pretty print the table
        :return: None
        """
        for index, entry in enumerate(self.table):
            if entry[1] is None:
                print('Entry: ', index, " Interval start: ", entry[0], " Successor: ", "None")
            else:
                print('Entry: ', index, " Interval start: ", entry[0], " Successor: ", entry[1].id)
