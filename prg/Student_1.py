# create a student class with rno, name, address and mark in 5 subject
# display total mark and percentage of mark

class Student:
    def __init__(self, name, rno , address, marks):
        self.name = name
        self.rno = rno
        self.address = address
        self.marks = marks

    def display(self):
        print('rno:', self.rno)
        print('name:', self.name)
        print('address:', self.address)
        print('marks:', self.marks)

        tot = sum(self.marks)
        per = tot/len(self.marks)

        print('total =', tot)
        print('averagae = %.2f' % per)
rno = int(input('enter roll no: '))
name = input('enter name: ')
address = input('enter address: ')
marks = [int(i) for i in input('enter marks: ').split(',')]

s1 = Student(name, rno , address, marks)
s1.display()
