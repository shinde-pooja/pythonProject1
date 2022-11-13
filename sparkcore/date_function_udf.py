from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc = spark.sparkContext


data = "E:\\big-data\\drivers-20220726T155648Z-001\\donations.csv"

df=spark.read.format("csv").option("header", "true").option('inferSchema', "true").load(data)

# i want udf to convert yo 3 yr , 4month, 9 days

# task1: create udf to get expected data format, like 1 yr , 2 month, 4 days
# task 2: every month 15th what day u'll get(sun or mon)

def nums(days):
    yr=days%365
    mn=
    dy=
    full =f"%yr year %mn month %dy days"