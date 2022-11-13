from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc = spark.sparkContext


data ="E:\\big-data\\datasets\\file1.csv"

df= spark.read.format("csv").option("header", "true").option('inferschema','true').load(data)

df1=df.groupBy(col('Name')).agg(collect_list("designation")).select(col(contact_info))


df1.show(truncate=False)



