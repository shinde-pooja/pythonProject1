from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc = spark.sparkContext

data="E:\\big-data\\datasets\\datasets\\books.xml"

df=spark.read.format("xml").option("path",data).option("rowTag","book").load()

res=df.withColumnRenamed("_id", "id").where(col("price")>10)
res.show()
op="E:\\big-data\\datasets\\output"
res.toPandas().to_csv(op)
#res.write.mode("overwrite").format("csv").option("header","true").save(op)