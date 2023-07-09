from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc = spark.sparkContext


data="E:\\big-data\\drivers-20220726T155648Z-001\\us-500.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema", "true").load(data)



win=Window.partitionBy(col("state")).orderBy(col("zip").desc())

ndf=df.withColumn("ntile_column",ntile(5).over(win))
ndf.show()

