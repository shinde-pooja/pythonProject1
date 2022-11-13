# inner class demo

class Student:
    def __init__(self):
        self.rno = 100
        self.name = 'lakshmi'

    def display(self):
        print('rno: ', self.rno)
        print('name:',self.name)

    class Dob:
        def __init__(self):
            self.dd = 14
            self.mm = 6
            self.yy = 1999

        def display(self):
            print('date of birth= {}/{}/{}'.format(self.dd,self.mm,self.yy))


st = Student()
st.display()

# to create inner class object
# 1st way
d = st.Dob()
d.display()
# 2nd way
e = Student().Dob()
e.display()