from unicodedata import normalize

import snappy

from config import ctx


COLORS = {
    'purple': '\033[95m',
    'blue': '\033[94m',
    'green': '\033[92m',
    'brown': '\033[93m',
    'red': '\033[91m',
    'end': '\033[0m',
}


def log(msg, color=None):
    color = COLORS.get(color)
    if color:
        print('%s%s%s' % (color, msg, COLORS['end']))
    else:
        print(msg)

def from_bytes(b):
    return int.from_bytes(snappy.decompress(b), 'big')

def to_bytes(i):
    if i == 0:
        size = 0
    else:
        size = 1 + i.bit_length() // 8
    return snappy.compress(i.to_bytes(size, 'big'))

def to_ascii(word):
    return normalize('NFKD', word).encode('ascii', 'ignore').lower()

def ranks(i):
    while i:
        l = i.bit_length() - 1
        if l == 0:
            return
        yield l
        i = i ^ (1 << l)

def get_match_context(idx, line):
    f = idx - 50
    t = idx + 50
    if f < 0:
        t = t - f
        f = 0
    return line[f:t]

def limit_offset():
    page = ctx.page
    limit = ctx.length
    offset = limit * page
    return limit, offset

def compress(data):
    if data is None:
        return None
    return snappy.compress(data.encode())

def decompress(data):
    if data is None:
        return None
    return snappy.decompress(data).decode()


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

