# multiple inheritance

class Father:
    def height(self):
        print('height = 6.0f')

class Mother:
    def color(self):
        print('color = tan')

class Child(Father, Mother):
    pass

ch = Child()
print('child quakities')
ch.height()
ch.color()