from threading import Thread
from json import dumps, loads
from time import sleep
from hashlib import md5
from pathlib import Path

def address(s):

    address = md5(s.encode()).hexdigest()[:16].upper().replace("", " ").split()
    path = "db"
    while address:
        if len(address) % 2 == 0:
            path += "/"
        path += address.pop(0)
    return path

class db():

    def __init__(self):

        self.queue = []
        self.cache = []
        self.read = []

        Thread(target=self.reader, daemon=True).start()
        Thread(target=self.writer, daemon=True).start()

    def push(self, data):

        if not isinstance(data, dict):
            raise TypeError("Push must be dict: {}")

        for obj in data:
            for name in obj:
                self.queue.append({obj:data[obj]})

    def pull(self, l):

        if not isinstance(l, list):
            raise TypeError("Pull must be list: []")

        for item in l:
            self.read.append(item)

        r = {}

        while len(r) < len(l):
            for obj in l:
                for item in self.cache:
                    for index in item:
                        if obj == index:
                            r.update(item)

        return r

    def drop(self, l):

        if not isinstance(l, list):
            raise TypeError("Drop must be list: []")

        for name in l:
            self.queue.append({name:None})

    def find(self):

        """Coming Soon: Command used to hunt for information across distributed databases."""
        pass

    def writer(self):

        while True:
            sleep(.1)
            while self.queue:
                data = self.queue.pop(0)
                for val in data:
                    p = address(val)
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
                        fob.write(dumps(data))
                    for item in self.cache:
                        for name in item:
                            if name == val:
                                self.cache.remove(item)

    def reader(self):

        while True:

            sleep(.1)
            while self.read:
                name = self.read.pop(0)

                found = 0

                for entry in self.cache:
                    if name in entry:
                        found = 1

                for entry in self.queue:
                    if name in entry:
                        self.cache.append(entry)
                        found = 1

                if not found:

                    try:
                        with open(address(name), "r") as fob:
                            data = loads(fob.read())
                            if name in data:
                                self.cache.append({name:data[name]})
                    except:
                        self.cache.append({name: None})

                while len(self.cache) > 100:
                    self.cache.pop(0)