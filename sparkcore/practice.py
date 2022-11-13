
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc = spark.sparkContext
#
# data="E:\\big-data\\drivers-20220726T155648Z-001\\drivers\\asl.csv"
# ardd= sc.textFile(data)
#
#
# # write a prg to convert lst=['a','b','c','d','d'] to {'a': 1, 'b': 1, 'c': 1, 'd': 2}
# lst=['a','b','c','d','d']
# print(lst)
#
# d={}
# for i in lst:
#     d[i]=lst.count(i)
# print(d)
#
# # 2. reverse the string
# str= 'pooja'
# # way 1
# rev=""
# for i in str:
#     rev= i+ rev
# print(rev)
# # way 2
# print(''.join(reversed(str)))
#
# a=[apple,banana,mango,apple]
#
# # a=[{apple:(1,4),banana:2,mango:3}]
# b={}
# for i,x in enumerate(a):
#     b[x]=i
#

# print(b)


# prg to find 2nd hight salary
data ="E:\\big-data\\drivers-20220726T155648Z-001\\10000Records.csv"

df= spark.read.format("csv").option("header", "true").option('inferschema','true').load(data)

'''

win= Window.orderBy(col('Salary').desc())

df1=df.withColumn("rnk", dense_rank().over(win)).filter(col("rnk")==2)

df1.show()


# 2nd hight salary using spark sql:

df.createOrReplaceTempView('tab')

sq= spark.sql("select * from (select e.*, dense_rank()over(order by Salary desc)rnk from tab e) where rnk=2 ")

sq.show()
'''
