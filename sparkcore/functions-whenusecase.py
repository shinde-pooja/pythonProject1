from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc = spark.sparkContext

data="E:\\big-data\\drivers-20220726T155648Z-001\\us-500.csv"

df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
df.show()
ndf1=df.withColumn("state", when(col("state")=="NY", "NewYork").otherwise("state"))
ndf2=df.withColumn("state", when(col("state")=="NY", "NewYork").when(col("state")=="CA", "Cali").otherwise(df.state))

ndf3=df.withColumn("address1", when(col("address").contains("#"),"******").otherwise(df.address))\
    .withColumn("address2", regexp_replace(col("address"), "#","_"))
ndf.printSchema()
ndf1.show()
ndf2.show()
ndf3.show()