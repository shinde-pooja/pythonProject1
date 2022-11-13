# interface to connect to any database

from abc import *
class Myinter(ABC):
    @abstractmethod
    def connect(self):
        pass

class oracle(Myinter):
    def connect(self):
        print('connectiong to oracle database...')

class sybase(Myinter):
    def connect(self):
        print('connecting to sybase database...')

str = input('which database ? ')
classname = globals()[str]
c = classname()
c.connect()