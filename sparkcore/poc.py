import re

from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc = spark.sparkContext


data = "E:\\big-data\\datasets\\datasets\\asl.csv"
'''
df=spark.read.format("csv").option("header", "true").option("inferschema","true").load(data)

df1=df.where(col("city")=="blr")
df1.show()
'''

import datetime
import re
d = datetime.date.today()

dt=str(str(d.day)+str(d.month)+str(d.year))
sstring=".csv"
print("initial_strings : ", data, "\nsubstring : ", sstring)
res = re.sub(sstring, '', data)

#d2=res+d
print(res)
#print(d2)

path="E:\\big-data\\datasets\\datasets\\"

file=path+"data"+dt+".csv"
print(file)

op_file = "s3://testbucket07sept/daily_rec_op/" + "opdata" + dt + ".csv"
print(op_file)
print(type(op_file))

