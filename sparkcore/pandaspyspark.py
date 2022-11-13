from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data = "E:\\big-data\\drivers-20220726T155648Z-001\\emailsmay4.txt"

erdd = spark.sparkContext.textFile(data)
# res=erdd.flatMap(lambda x: x.split(" ")).filter(lambda x: "@" in x)

res=erdd.filter(lambda x: "@" in x).map(lambda x:x.split(" ")).map(lambda x: (x[0], x[-1]))


for i in res.collect():
    print(i)