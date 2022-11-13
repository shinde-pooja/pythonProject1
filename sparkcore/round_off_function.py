from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc = spark.sparkContext
data = "E:\\big-data\\drivers-20220726T155648Z-001\\donations.csv"

df=spark.read.format("csv").option("header", "true").option('inferSchema', "true").load(data)

from pyspark.sql.types import *

ndf= df.withColumn("dt", to_date(df.dt,"d-M-yyyy"))\
.withColumn("monbet", months_between(current_date(),col("dt")))\
    .withColumn("floor", floor(col("monbet")))\
    .withColumn("ceil", ceil(col("monbet")))\
    .withColumn("round", round(col("monbet")).cast(IntegerType()))\
    .withColumn("dttrunc", date_trunc("year", col("dt")))\
    .withColumn("montruc", date_trunc("mon", col("dt")))\
    .withColumn("weekofyr", weekofyear(col("dt")))

ndf.show(truncate=False)