from constants import *


class FingerTable:
    def __init__(self, id):
        self.table = []
        for i in range(m):
            x = pow(2, i)
            entry = (id + x) % pow(2, m)
            node = None
            self.table.append([entry, node])

    def print(self):
        for index, entry in enumerate(self.table):
            if entry[1] is None:
                print('Entry: ', index, " Interval start: ", entry[0], " Successor: ", "None")
            else:
                print('Entry: ', index, " Interval start: ", entry[0], " Successor: ", entry[1].id)
