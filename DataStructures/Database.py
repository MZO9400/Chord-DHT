class Database:
    """
    In-memory data store for node
    """
    def __init__(self):
        self.data = {}

    def insert(self, key, value):
        """
        Inserts a kv pair into data structure
        :param key: hashed key of the data
        :param value: data
        :return: None
        """
        self.data[key] = value

    def delete(self, key):
        """
        Delete the data at a given key
        :param key: hashed key of the data
        :return: None
        """
        del self.data[key]

    def search(self, search_key):
        """
        Searches the data at a given key
        :param search_key: hashed key of the data
        :return: if not found: None, if search key exists: return data
        """
        return self.data[search_key] if search_key in self.data else None
