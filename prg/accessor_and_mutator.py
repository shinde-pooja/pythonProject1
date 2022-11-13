# accessor and mutator methods

class Employee:
    # constrctor
    def __init__(self):
        self.name = 'srinu'
        self.sal = 34500.75

    # accessor methods
    def getName(self):
        return self.name

    def getSal(self):
        return self.sal

    # mutator methods
    def setName(self,name):
        self.name = name

    def setSal(self, sal):
        self.sal = sal



e = Employee()
str = e.getName()
print(str)
e.setName('lakshmi')
str = e.getName()
print(str)

str1 = e.getSal()
print(str1)
e.setSal(500000)
str1=e.getSal()
print(str1)