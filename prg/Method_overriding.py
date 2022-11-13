# method overriding

class Super:
    def display(self, x):
        print('this is super class method',x)

class Sub(Super):
    def display(self, y):
        print('square = ', y*y)

s = Sub()
s.display(3)
s =  Super()
s.display(3)