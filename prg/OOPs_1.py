# class and object
'''
class Person:
    # properties = vars
    def __init__(self):
        self.name= 'pooja'
        self.age = 22

    # action / methods
    def talk(self):
        print('hello, i am' , self.name)
        print('my age is', self.age)
        print(self)

p1= Person()
p1.talk()
print(p1)

p2 = Person()
p2.talk()
'''

class Person:
    def __init__(self, n, a):
        self.name = n
        self.age = a

    def talk(self):
        print('hello, i am',self.name)
        print('age is', self.age)

p1 = Person('pooja',22)
p1.talk()
