from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc = spark.sparkContext

data="E:\\big-data\\drivers-20220726T155648Z-001\\us-500.csv"

df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)


#withColumn used to add new column (if column not exists) or update (if already column exists)
# lit(value) used to add something dummy value

 from pyspark.sql.types import *
 ndf=df.withColumn("age",lit(18)).withColumn("fullname",concat_ws(" ",df.first_name, df.last_name, df.state))\
    .withColumn("phone1",regexp_replace(col("phone1"),"-","").cast(LongType()))\
    .withColumn("phone2",regexp_replace(col("phone2"),"-","").cast(LongType()))\
    .drop("emaill", "city","country",'address')\
    .withColumnRenamed("first_name", "fname").withColumnRenamed("last_name","lname")


ndf.show()
ndf.printSchema()

