from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc = spark.sparkContext

data ='E:\\big-data\\datasets\\datasets\\zips.json'

df = spark.read.format("json").load(data)

ndf=df.withColumnRenamed("_id","id").withColumn("lang", col("loc")[0])\
    .withColumn("lati", col("loc")[1]).drop(col("loc"))
# explode simple explode unnest data means remove arrays elements

ndf.show()
ndf.printSchema()


ndf.write.mode("overwrite").option("header","true").format("csv").save("E:\\big-data\\datasets\\output\\jsontocsv")