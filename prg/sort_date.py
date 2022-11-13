# sorting dates
from datetime import *

n = int(input('how many dates? '))
for i in range(n):
    d,n,y = [int(i) for i in input('enter date(dd/mm/yyyy): ').split('/')]
    dt = date(y,n,d)
    lst.append(dt)

print('sorted date')
lst.sort()
for i in lst:
    print(i)