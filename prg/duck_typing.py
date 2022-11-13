# duck typing

class Duck:
    def talk(self):
        print('Quack quack!')

class Dog:
    def talk(self):
        print('Bow wow!')

def call_talk(obj):
    obj.talk()      # duck typing

d = Duck()
call_talk(d)

d = Dog()
call_talk(d)