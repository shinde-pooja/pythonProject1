# abstract demo
from abc import ABC , abstractmethod
class Myclass(ABC):
    def display(self):
        print('this is concrete method')

    @abstractmethod
    def cal(self, x):
        pass

class Sub1(Myclass):
    def cal(self, x):
        print('square = ', x * x)

import math
class Sub2(Myclass):
    def cal(self, x):
        print('square root =', math.sqrt(x))

s1 = Sub1()
s1.display()
s1.cal(16)

s2 = Sub2()
s1.display()
s2.cal(16)