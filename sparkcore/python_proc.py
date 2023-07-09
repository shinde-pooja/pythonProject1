from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc = spark.sparkContext

# ---------------------- get the max duplicate count -----------------
#
# str='pooja'
# lst=list(str)
#
# dict={i:lst.count(i) for i in lst}
# print(dict)
#
# d= sorted(dict.items(), key= lambda items:items[1])            # sort the dict based on value, here we used lambda
# function print(d)
#
# print(d[-1])            # print last key-value pair
#
# t= list(d)
# for i in t:
#     print(i)

# --------------------- acronym ------------------------------------

# 1. write a python prg to find extract the acronym from that string
#  input = 'have fun coding'
#  output = 'HFC'
#
# str= 'have fun coding'
#
# lst= str.split()
# print(lst)
# j=''
# for i in lst:
#     j+= i[0].upper()
# print(j)


# ------------------- write a prg to extract a specific portion of a string based on delimiter ------------------
input = 'apple-banana-cherry'
# delimiter = '-'
# output = 'banana'

lst= input.split('-')
print(lst[1])



# write a prg to find most common word in string
input_str= 'hello world'

lt= list(input_str)
print(lt)

d={i: lt.count(i) for i in lt}
print(d)

di= sorted( d.items(), key= lambda items: items[1])
print(di[-1])


# -------------------------- show the no who's sum will get 7 ---------------
# p=[]
# l = [1,2,3,4,5,6,6]
#
# print(l.index(2))
#
# for i in l:
#     for j in l:
#         if i + j ==7:
#
#             c= (l.index(i), l.index(j))
#             p.append(c)
# k =[]
# for i in p:
#     if i not in k:
#         k.append(i)
#
# print(k)
#
# #
# -- find the index of num which additon will give result of 7
# l = [1, 2, 3, 4, 5, 6, 6]
# result = []
#
# for i in range(len(l)):
#     for j in range(i + 1, len(l)):
#         if l[i] + l[j] == 7:
#             result.append((i, j))
#
# print(result)
#
# print(l[2])
#
# for i in range(8):
#     print(i)


# ------------ reverse the string
#
# str = 'pooja'
#
# print(str[::-1])
#
# str= "pooja"
# print(str)
#
# name= str
# print(name)
#
#
#
# name= 'sachin'
# print(name)
# print(str)
# str='nitin'
# print(str)
# print(name)
#
# a1=[1,2,3,4,5]
#
# print(a1)
#
# a2=a1
# print(a2)
#
# a2=[9,8,7,6]
# print(a2)
# print(a1)

import numpy as np

s1= np.array([1,2,3,4,5])
print(s1)

s2=s1
print(s2)

s2[1]=87

print(s2)
print(s1)


