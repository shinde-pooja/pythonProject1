# WAP to create square class
# derive rectange class from it areas of both the square and rectangle
# derive rectange class from square class

class Square:
    def __init__(self, x):
        self.x = x

    def area(self):
        print("area of square: ", self.x * self.x)


class Rectangle(Square):
     def __init__(self, x, y):
         super().__init__(x)
         self.y = y

     def area(self):
         super().area()
         print('area of rectangle: ', self.x * self.y)

r1 = Rectangle(4,34)
r1.area()
