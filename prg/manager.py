'''
create a manager class with id , name, deptname and annual salary
display the manager's name, annual salary, income tax, HRA

note:
if annual salary <= 2,50,000 then income tax = 0%
if annual salary > 2,50,000 then income tax = 10 %
 hra = 15.5 % of annual salary
'''

class Manager:

    def __init__(self, id, name, dept_name, annual_sal):
        self.id = id
        self.name = name
        self.dept_name = dept_name
        self.annual_sal = annual_sal


    def display(self):
        if self.annual_sal <= 250000:
            tax = self.annual_sal
        else:
            tax = self.annual_sal * 10/100

        hra = self.annual_sal * 15.5/100

        print('id:',self.id)
        print('name:',self.name)
        print('department name:', self.dept_name)
        print('annual salary :', self.annual_sal)
        print('income tax :', tax)
        print('hra: ', hra)

m1 = Manager(101, 'pooja', 'development', 3000000)
m1.display()