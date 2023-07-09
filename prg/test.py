from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc = spark.sparkContext

# import numpy as np
#
# a1= np.array([1,2,3,4,5])
#
# print(a1)
#
# a2=a1
# print(a2)
#
# a2[0]=0
# print(a1)
#
# a2[3]=0
# print(a1)
#
# a3=a2.view()
#
# a4=a2.copy()
# a2[2]=0
#
# print('a2',a2)
# print('a1',a1)
# print('a3 view',a3)
# print('a4 copy',a4)
#

x=2
y=7

print(z)


