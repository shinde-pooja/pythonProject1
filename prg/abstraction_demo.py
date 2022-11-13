# abstraction
class Bank:
    def __init__(self):
        self.accno = 1001
        self.name = 'srinu'
        self.addr = 'HNO-44/A, nera Prozone, Aurangabad'
        self.phone = 1234567899
        self.bal = 56000.00
        self.__loan = 1500000

    def display_to_clerk(self):
        print('accno:' , self.accno)
        print('name:',self.name)
        print('address:',self.addr)
        print('phone no:',self.phone)
        print('balance amt: Rs.%.2f'%self.bal)

b = Bank()
b.display_to_clerk()
# print(b.loan)
print(b._Bank__loan)   # name mangiling
