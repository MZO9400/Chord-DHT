class Database:
    def __init__(self):
        self.data = {}

    def insert(self, key, value):
        self.data[key] = value

    def delete(self, key):
        del self.data[key]

    def search(self, search_key):

        if search_key in self.data:
            return self.data[search_key]
        else:
            return None
