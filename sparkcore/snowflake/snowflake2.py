from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").config("spark.jars","E:\\bigdata\\spark-3.1.2-bin-hadoop3.2\\jars\\spark-snowflake_2.12-2.11.0-spark_3.1").getOrCreate()

sc=spark.sparkContext
sc._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.disablePushdownSession(sc._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate())

# You might need to set these
sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "AKIA6ANABO7AVRAVMU6T")
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "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")

sfOptions ={
"sfURL" : "tv61834.ap-southeast-1.snowflakecomputing.com",
  "sfUser" : "PoojaShinde",
  "sfPassword" : "Pooja@2812",
  "sfDatabase" : "poojadb",
  "sfSchema" : "PUBLIC",
  "sfWarehouse" : "SMALL"
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
#net\snowflake\spark\snowflake
'''
df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
  .options(**sfOptions) \
  .option("query",  "select * from CUSTOMER")\
  .option("autopushdown", "off") \
  .load()
'''
data="E:\\big-data\\drivers-20220726T155648Z-001\\drivers\\bank-full.csv"
df=spark.read.format("csv").option("header","true").option('sep',';').option("inferSchema",'true').load(data)

df.show()
df.write.format(SNOWFLAKE_SOURCE_NAME) \
  .options(**sfOptions) \
  .option("dbtable",  "banktab")\
  .save()

#https://docs.snowflake.com/en/user-guide/spark-connector-use.html
#https://mvnrepository.com/artifact/net.snowflake/snowflake-jdbc/3.13.14
#https://mvnrepository.com/artifact/net.snowflake/spark-snowflake_2.12/2.11.0-spark_3.1