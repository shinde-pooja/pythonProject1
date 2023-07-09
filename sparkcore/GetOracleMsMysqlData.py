
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import configparser
from configparser import ConfigParser
conf=ConfigParser()vzsaq
conf.read(r"E:\\big-data\\files\\config.txt")
host=conf.get("cred","host")
user=conf.get("cred","user")
pwd=conf.get("cred","pass")

spark = SparkSession.builder.master("local[*]").appName("testing").getOrCreate()
'''
host="jdbc:mysql://mysqldb.czpjgncya0hs.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSl=false"

uname="myusername"
pwd="mypassword"
'''


df=spark.read.format("jdbc").option("url",host)\
    .option("user",user).option("password",pwd)\
    .option("dbtable","emp").option("driver","com.mysql.jdbc.Driver").load()
#df.show()

# process data
from pyspark.sql.types import *

res=df.na.fill(0,["comm"]).withColumn("comm", col("comm").cast(IntegerType()))\
    .withColumn("hiredate", date_format(col("hiredate"), "yyyy/MMM/dd"))

res.write.mode("overwrite").format("jdbc").option("url",host).option("user",user)\
    .option("password",pwd).option("dbtable", "empclean").option("driver","com.mysql.jdbc.Driver").save()












