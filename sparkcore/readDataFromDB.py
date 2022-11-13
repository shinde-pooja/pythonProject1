from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

host="jdbc:mysql://mysqldb.czpjgncya0hs.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSl=false"
user="myusername"
pwd="mypassword"
df=spark.read.format("jdbc").option("url",host).option("user",user).option("password",pwd).option("dbtable",'emp')\
    .option("driver",'com.mysql.jdbc.Driver').load()
res=df.na.fill(0,['comm'])
res.show()

# res.write.format("csv").option("header", "true").save("")      #to store data

