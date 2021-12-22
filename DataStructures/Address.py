class Address:
    """
    Address class is a data structure to store and load IP Addresses of a node
    """
    def __init__(self, ip, port):
        """

        :param ip: string parameter for storing IP
        :param port: number parameter for storing port
        """
        self.ip = ip
        self.port = port

    def __str__(self):
        """
        data structure to string function overload
        :return: string of IP in IP:port format
        """
        return self.ip + ":" + str(self.port)
