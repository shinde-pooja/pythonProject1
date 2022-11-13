from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()

df=spark.read.format("org.apache.spark.sql.cassandra").option('table','asl').option("keyspace","cassdb").load()
df.show()
