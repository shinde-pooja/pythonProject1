from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc = spark.sparkContext

data = "E:\\big-data\\drivers-20220726T155648Z-001\\10000Records.csv"
df=spark.read.format("csv").option("header", "true").option("inferschema","true").load(data)

df.show(5,truncate=False)
# bydefault show method showing top 20 rows and if any fields having 20 char its truncates and show ...

#num= int(df.count())
#df.show(num,truncate=False) #to show all rows with without truncate
#-------------------------------------------------------------
import re
cols=[re.sub('[^a-zA-Z0-1]',"",c) for c in df.columns]
# # re : replace  except all small letter ,capital letter and number except those anay other symbol if we have remove
#
ndf=df.toDF(*cols)
# # toDF used to rename all column and convert rdd to dataframe
#
ndf.show(21,truncate=False)
# ndf.printSchema()
# # dataframe column name and its datatype display properties
#
#res = ndf.groupby(col("gender")).agg(count("*").alias("cnt"))
#
res=ndf.withColumn("DateofBirth", to_date(col("DateofBirth"),"M/d/yyyy")).withColumn("today", current_date())\
    .withColumn("oldage", datediff(col("today"), col("DateofBirth"))).where(col("Gender")=="F")
res.show()