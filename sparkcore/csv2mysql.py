from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc = spark.sparkContext

import configparser
from configparser import ConfigParser
conf=ConfigParser()
conf.read(r"E:\\big-data\\files\\config.txt")
host=conf.get("cred","host")
user=conf.get("cred","user")
pwd=conf.get("cred","pass")

data=conf.get("input","indata")
opdata=conf.get("input","opdata")

df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)


import re

cols=[re.sub('[^a-zA-Z0-9]', "",c) for c in df.columns]

res=df.toDF(*cols)

res.show()
res.write.format("jdbc").option("url",host).option("user",user).option("password",pwd)\
    .option("dbtable","1000cleanRecord").option("driver","com.mysql.jdbc.Driver").save()
