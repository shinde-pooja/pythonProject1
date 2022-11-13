from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc = spark.sparkContext
data="E:\\big-data\\datasets\\datasets\\zips.json"
df = spark.read.format("json").load(data)

ndf=df.withColumnRenamed("_id","id").withColumn("lang", col("loc")[0])\
    .withColumn("lati", col("loc")[1]).drop(col("loc"))

ndf.createOrReplaceTempView("tab")
ndf1=spark.sql("select * from tab where state ='CA'")

host="jdbc:mysql://mysqldb.czpjgncya0hs.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSl=false"
user="myusername"
password="mypassword"

ndf1.write.format("jdbc").option("url",host).option("user",user).option("password",password).option("dbtable","json").save()
