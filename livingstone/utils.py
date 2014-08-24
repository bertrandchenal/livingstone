from bz2 import compress as bz2_compress
from bz2 import decompress

compress = lambda a: bz2_compress(a, 1)


def from_bytes(b):
    return int.from_bytes(decompress(b), 'big')

def to_bytes(i):
    if i == 0:
        size = 0
    else:
        size = 1 + i.bit_length() // 8
    return compress(i.to_bytes(size, 'big'))


def ranks(i):
    while i:
        l = i.bit_length() - 1
        if l == 0:
            return
        yield l
        i = i ^ (1 << l)


class LRU:

    def __init__(self, size=1000, discard=None):
        self.fresh = {}
        self.stale = {}
        self.size = size
        self.discard = discard

    def get(self, key, default=None):
        if key in self.fresh:
            return self.fresh[key]

        if key in self.stale:
            value = self.stale[key]
            # Promote key to fresh dict
            self.set(key, value)
            return value
        return default

    def clean(self, full=False):
        if self.discard:
            # discard staled values
            for key, value in self.stale.items():
                if key in self.fresh:
                    if full:
                        value = self.fresh.pop(key)
                    else:
                        continue
                self.discard(key, value)

            # Full cleanup: discard also fresh content
            if full:
                for key, value in self.fresh.items():
                    self.discard(key, value)

        # Fresh is put in stale, new empty fresh is created
        self.stale = self.fresh
        self.fresh = {}

    def set(self, key, value):
        self.fresh[key] = value
        if len(self.fresh) > self.size:
            self.clean()

    def close(self):
        self.clean(full=True)


class Store:

    def __init__(self, **data):
        self.__dict__.update(data)
