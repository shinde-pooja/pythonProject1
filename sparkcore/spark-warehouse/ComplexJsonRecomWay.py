from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc = spark.sparkContext
