from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc=spark.sparkContext
#data =[12,32,34,4,54,26]
#drdd=spark.sparkContext.parallelize(data)


data = "E:\\big-data\\drivers-20220726T155648Z-001\\drivers\\asl.csv"
aslrdd = spark.sparkContext.textFile(data)
#or
#aslrdd = sc.textFile()
res=aslrdd.map(lambda x:x.split(",")).filter(lambda x:'hyd' in x[2])

res1=aslrdd.map(lambda x:x.split(","))
#filter by default apply a logic/filter on top of entire line
#filteralmost in sql ur using where condition to filter result similarly ur using filter function

for i in res.collect():
    print(i)

print("------------------------------------")

for i in res1.collect():
    print(i)
print('---------------------------')


res2=aslrdd.filter(lambda x: 'age' not in x).map(lambda x:x.split(","))
print('---------------------------')
for i in res2.collect():
    print(i)

print('-----------------------------')
res3=aslrdd.filter(lambda x: 'age' not in x).map(lambda x:x.split(",")).filter(lambda x:'hyd' in x[2])
print('---------------------------')
for i in res3.collect():
    print(i)


