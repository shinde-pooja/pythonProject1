# search if the str begins with 'He'
import re
str = 'Hello world!'

######## start with ############
obj = re.search(r'^He',str)

if obj:
    print('yes')
else:
    print('no')

########### end with ##############
obj = re.search(r'!$SSS', str)

if obj:
    print('yes')
else:
    print('no')
