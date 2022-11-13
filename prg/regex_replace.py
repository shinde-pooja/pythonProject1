# regex to replace Ahmedabad with Nasik

str = 'kumbhmela will be conducted at Ahmedabad in india. Ahmedabad is really good'

import re
str1 = re.sub(r'Ahmedabad','Nashik',str)
print(str1)