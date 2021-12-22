import threading
import unittest

import Client
import spawner
from constants import ip


class Tests(unittest.TestCase):
    """
    Test cases to automatically run the system
    """

    def test(self):
        """
        The actual test case
        If this test cases passes: It confirms that the DHT is working correctly and
        all nodes can communicate to each other
        :return: None
        """
        threads = [
            threading.Thread(target=lambda: spawner.create(3000)),
            threading.Thread(target=lambda: spawner.join(3010, 3000)),
            threading.Thread(target=lambda: spawner.join(3020, 3000)),
            threading.Thread(target=lambda: spawner.join(3030, 3000))
        ]  # create 4 threads (nodes): one creates a network and the rest join it
        for thread in threads:  # start all nodes as daemon threads
            thread.daemon = True
            thread.start()
        Client.client(ip, '3000', 'insert', ['1', '2'])
        print("Insert KV { 1: 2 } on Client 1 @ port 3000")
        Client.client(ip, '3010', 'insert', ['2', '3'])
        print("Insert KV { 2: 3 } on Client 2 @ port 3010")
        Client.client(ip, '3020', 'insert', ['3', '4'])
        print("Insert KV { 3: 4 } on Client 3 @ port 3020")
        Client.client(ip, '3030', 'insert', ['4', '5'])
        print("Insert KV { 4: 5 } on Client 4 @ port 3030")
        Client.client(ip, '3000', 'insert', ['5', '6'])
        print("Insert KV { 5: 6 } on Client 1 @ port 3000")
        self.assertEqual('3', Client.client(ip, '3000', 'search', ['2']))  # find value at key 2 from node 3000
        print("Test for Key 2 = value 3 on Client 1 @ port 3000")
        self.assertEqual('4', Client.client(ip, '3000', 'search', ['3']))  # find value at key 5 from node 3000
        print("Test for Key 3 = value 4 on Client 1 @ port 3000")


if __name__ == '__main__':
    unittest.main()
