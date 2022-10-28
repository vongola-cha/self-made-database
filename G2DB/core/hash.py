class HashTable:
    def __init__(self, size):
        self.elem = [None for i in range(size)]
        self.count = size

    def hash(self, key):
        return key % self.count

