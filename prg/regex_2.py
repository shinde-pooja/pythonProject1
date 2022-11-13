# regex to find all words starting with 'an' and 'ak'

str = 'anil akhil anant arun arati arundhati abhijit ankur apple'
import re
lst = re.findall(r'a[nk]\w+',str)
print(lst)