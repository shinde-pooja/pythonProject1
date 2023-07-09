from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc= spark.sparkContext
data = "E:\\big-data\\drivers-20220726T155648Z-001\\donations.csv"
drdd = sc.textFile(data)


pro = drdd.filter(lambda x: "dt" not in x).map(lambda x: x.split(",")).map(lambda x: (x[0], int(x[2])))

for i in pro.collect():
    print(i)

print("-----------------------")
pro1 = drdd.filter(lambda x: "dt" not in x).map(lambda x: x.split(",")).map(lambda x: (x[0], int(x[2]))).reduceByKey(lambda x,y : x+y)

for i in pro1.collect():
    print(i)

print("---------------------")
pro2 = drdd.filter(lambda x: "dt" not in x).map(lambda x: x.split(",")).map(lambda x: (x[0], int(x[2]))).reduceByKey(lambda x,y : x+y).sortBy(lambda x: x[1], ascending=False)

for i in pro2.collect():
    print(i)
print("---------------------")

# select distinct name from donation
pro3 = drdd.filter(lambda x: "dt" not in x).map(lambda x: x.split(",")).map(lambda x: x[0]).distinct()

for i in pro3.collect():
    print(i)