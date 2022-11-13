from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc=spark.sparkContext

data ="E:\\big-data\\drivers-20220726T155648Z-001\\drivers\\asl.csv"

aslrdd = sc.textFile(data)

#res=aslrdd.filter(lambda x: 'hyd' in x)

#res=aslrdd.map(lambda x: x.split(",")).filter(lambda x: 'hyd' in x)

#res=aslrdd.map(lambda x : x.split(","))

#res=aslrdd.filter(lambda x: 'age' not in x).map(lambda x : x.split(","))

res=aslrdd.filter(lambda x: 'age' not in x).map(lambda x : x.split(",")).filter(lambda x: 'hyd' in x)


for i in res.collect():
    print(i)
print("------------------------------------")
res1 = aslrdd.filter(lambda x : "age" not in x).map(lambda x: x.split(",")).map(lambda x:(x[2], 1)).reduceByKey(lambda x, y: x+y)


for i in res1.collect():
    print(i)


