from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc = spark.sparkContext

data="E:\\big-data\\drivers-20220726T155648Z-001\\us-500.csv"

df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)


ndf=df.withColumn("substr", substring(col("email"),0,5))
ndf.show()
print("---------------------------")

ndf1= df.withColumn("substr", substring(col("email"),0,5)).withColumn("username", substring_index(col("email"),"@",-1))\
 .withColumn("username1", substring_index(col("email"),"@",1))
ndf1.show()
print("---------------------------")

ndf2=ndf1.groupBy(col("username")).agg(count("*").alias("cnt")).orderBy(col("cnt").desc())

#above is same as below
#ndf1=ndf.groupBy(col("username")).count().orderBy(col("count").desc())
ndf2.show()
print("---------------------------")

df.createOrReplaceTempView("tab")
#ndf = spark.sql("select *, concat_ws(' ', first_name, last_name) fullname, substring_index(email,'@', 1) username from tab")

qry ="""with tmp as (select *, concat_ws(' ', first_name, last_name) fullname, substring_index(email,'@', -1) mail from tab)
select mail, count(*) cnt from tmp group by mail order by cnt desc
"""
qdf=spark.sql(qry)


qdf.printSchema()
qdf.show()

# 121@airtel.com



