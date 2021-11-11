import threading
import unittest

import Client
import spawner


class Tests(unittest.TestCase):
    def test(self):
        t1 = threading.Thread(target=lambda: spawner.create(3000))
        t1.start()
        t2 = threading.Thread(target=lambda: spawner.join(3010, 3000))
        t2.start()
        t3 = threading.Thread(target=lambda: spawner.join(3020, 3000))
        t3.start()
        t4 = threading.Thread(target=lambda: spawner.join(3030, 3000))
        t4.start()
        Client.client("127.0.0.1", '3000', 'insert', ['1', '2'])
        Client.client("127.0.0.1", '3010', 'insert', ['2', '3'])
        Client.client("127.0.0.1", '3020', 'insert', ['3', '4'])
        Client.client("127.0.0.1", '3030', 'insert', ['4', '5'])
        Client.client("127.0.0.1", '3000', 'insert', ['5', '6'])
        self.assertEqual('3', Client.client("127.0.0.1", '3000', 'search', ['2']))
        self.assertEqual('4', Client.client("127.0.0.1", '3000', 'search', ['3']))


if __name__ == '__main__':
    unittest.main()
