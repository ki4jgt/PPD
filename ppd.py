from collections import OrderedDict
from threading import Thread
from json import dumps, loads
from time import sleep
from hashlib import md5
from pathlib import Path

def address(s, **kwargs):
    path = ""
    address = md5(s.encode()).hexdigest()[:16].upper()
    if "path" in kwargs:
        path = kwargs["path"]
    return path + "/".join([address[i*2:i*2+2] for i in range(8)])

class db:

    def __init__(self, **kwargs):
        
        self.path = ""
        self.memsize = 1000
        
        if "path" in kwargs:
            self.path = kwargs["path"]
        if "memsize" in kwargs:
            self.memsize = kwargs["memsize"]

        self.queue = OrderedDict()
        self.cache = OrderedDict()
        self.read = []

        Thread(target=self.reader, daemon=True).start()
        Thread(target=self.writer, daemon=True).start()

    def push(self, data):

        if not isinstance(data, dict):
            raise TypeError("Push must be dict: {}")

        self.queue.update(data)

    def pull(self, l):

        if not isinstance(l, list):
            raise TypeError("Pull must be list: []")

        self.read += l

        r = {}

        while len(r) < len(l):
            for obj in l:
                if obj in self.cache:
                    r.update({obj:self.cache[obj]})

        return r

    def drop(self, l):

        if not isinstance(l, list):
            raise TypeError("Drop must be list: []")

        for name in l:
            self.queue[name] = None

    def find(self):

        """Coming Soon: Command used to hunt for information across distributed databases."""
        pass

    def writer(self):

        while True:
            sleep(.1)
            while self.queue:
                data = self.queue.popitem(last=True)
                for val in data:
                    p = address(val, path = self.path)
                    Path(p[:-2]).mkdir(parents=True, exist_ok=True)
                    try:
                        with open(p, "r") as fob:
                            f = loads(fob.read())
                            f.update(data)
                            data = f

                    except:
                        with open(p, "a") as fob:
                            pass

                    with open(p, "w") as fob:
                        print(dict((data,)))
                        fob.write(dumps(dict((data,))))
                    if val in self.cache:
                        del self.cache[val]
                    self.cache.update((data,))

    def reader(self):

        while True:
            sleep(.1)
            while self.read:
                name = self.read.pop(0)

                found = 0

                if name in self.cache:
                    found = 1

                if name in self.queue:
                    self.cache[name] = self.queue[name]
                    found = 1

                if not found:

                    try:
                        with open(address(name, path = self.path), "r") as fob:
                            data = loads(fob.read())
                            if name in data:
                                self.cache[name] = data[name]
                    except:
                        self.cache[name] = None

                while len(self.cache) > self.memsize:
                    self.cache.popitem(last=True)