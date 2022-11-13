from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc = spark.sparkContext

import configparser
from configparser import ConfigParser
conf = ConfigParser()
conf.read(r"E:\\big-data\\files\\config.txt")
host=conf.get("cred","host")
user=conf.get("cred","user")
pwd=conf.get("cred", "pass")
tabs=["banktab","emp","dept"]


for i in tabs:
    df = spark.read.format("jdbc").option("url", host).option("user", user).option("password", pwd) \
        .option("dbtable", i).option("driver", "com.mysql.jdbc.Driver").load()
    df.show()

