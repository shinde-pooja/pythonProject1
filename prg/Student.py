
# create a student class with rno, name, address and mark in 5 subject
# display total mark and percentage of mark

class Student:
    def __init__(self):
        self.name = 'pooja'
        self.rno = 100
        self.address = 'aurangabad'
        self.marks = [77,65,76,56,86]

    def display(self):
        print('name:', self.name)
        print('rno:',self.rno)
        print('address:', self.address)
        print('marks:', self.marks)

        tot = sum(self.marks)
        avg = tot/len(self.marks)

        print('total =', tot)
        print('averagae = %.2f' % avg)

s1 = Student()
s1.display()
