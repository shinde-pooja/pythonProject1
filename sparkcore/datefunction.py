from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").config("spark.sql.session.timeZone", "EST").getOrCreate()
sc = spark.sparkContext
data = "E:\\big-data\\drivers-20220726T155648Z-001\\donations.csv"

df=spark.read.format("csv").option("header", "true").option('inferSchema', "true").load(data)

# spark default date format "yyyy-MM-dd" format only
#config("spark.sql.session.timeZone", "EST") ... its very imp based on original client date all default time based on us time only.at that time mention "EST"

res=df.withColumn("dt", to_date(df.dt,"d-M-yyyy"))\
    .withColumn("today", current_date())\
    .withColumn("ts", current_timestamp())\
    .withColumn("dtdiff", datediff(col("today"),col("dt")))\
    .withColumn("dtadd", date_add(col("dt"), 100))\
    .withColumn("dtsub", date_sub(col("dt"),100))\
    .withColumn("lastdt", last_day(col("dt")))\
    .withColumn("nextdt", next_day(col('today'),'Sunday'))\
    .withColumn("dtformat", date_format(col("dt"), "dd/MMM/yyyy/E/zzz"))\
    .withColumn("doweek", dayofweek(col('dt')))\
    .withColumn("dom", dayofmonth(col("dt")))\
    .withColumn("doy", dayofyear(col("dt")))\
    .withColumn("monbet", months_between(current_date(),col('dt')))\
    .withColumn("dttrunc", date_trunc("year", col("dt")))\
    .withColumn("montruc", date_trunc("mon", col("dt")))\
    .withColumn("weekofyr", weekofyear(col("dt")))















res.printSchema()
res.show(truncate=False)



