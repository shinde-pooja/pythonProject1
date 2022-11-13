# method overloading

class Myclass:
    @staticmethod
    def add(*x):
        tot = sum(x)
        print('sum =',tot)

Myclass.add(10,20)
Myclass.add(10,11,22)
Myclass.add(10,23,43,54,53)

