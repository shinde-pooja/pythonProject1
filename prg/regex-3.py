# regex to split a string where number are found

import re
str = 'gopi 2222 vinay 9988 subha rao 8989 akhil 230'


lst = re.split(r'\d+',str)
print(lst)

############################


lst = re.split(r'\D+',str)
print(lst)