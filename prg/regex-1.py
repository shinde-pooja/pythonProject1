# regex to retrive only date of births

str = 'vijay 20 1-5-2001, rohit 21 22-10-1990, sita 22 15-09-2000'
import re
lst = re.findall(r'\d{1,2}-\d{1,2}-\d{4}',str)
print(lst)

rln = re.findall(r' \d{2} ',str)
print(rln)

name = re.findall(r'\D+',str)
print(name)