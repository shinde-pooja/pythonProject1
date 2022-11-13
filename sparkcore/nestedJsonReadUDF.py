from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc = spark.sparkContext

data="E:\\big-data\\datasets\\datasets\\world_bank.json"
df=spark.read.format("json").option("header","true").load(data)
df.show()
df.printSchema()
#
# def json_read(df):
#     column_list=[]
#     if isinstance(column_name,)

