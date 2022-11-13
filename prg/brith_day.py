# to know the weak day depending on the birthay
from datetime import *
d, m, y = [int(i) for i in input('enter your birthdate(dd/mm/yyyy): ').split('/')]

dt = date(d,m,y)
str = dt.strftime('you were born on %A and it is %jth day in th year')

print(str)