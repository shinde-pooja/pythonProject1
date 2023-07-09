# a regular expressiom to search for string startung with m and having total 3 character

import re
str = 'nap man sun mop run mat rat nut'

######## search ######
obj = re.search(r'm\w\w',str)
# print(obj)
if obj:
    print(obj.group())
else:
    print('Not found')
print('--------------')
########### findall() #############

lst = re.findall(r'm\w\w',str)
print(lst)
for i in lst : print(i)

print('--------------')
########## match() ############

obj = re.match(r'm\w\w',str)
if obj:
    print(obj.group())
else:
    print('Not found')

print('--------------')
