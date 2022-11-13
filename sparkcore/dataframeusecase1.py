from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc = spark.sparkContext

#data="E:\\big-data\\drivers-20220726T155648Z-001\\donations.csv"

#df=spark.read.format("csv").load(data)
#df=spark.read.format("csv").option("header","true").load(data)
# if u mention header ture first line of data consider as column name

data="E:\\big-data\\drivers-20220726T155648Z-001\\donations1.csv"
rdd=spark.sparkContext.textFile(data)

skip=rdd.first()
odata=rdd.filter(lambda x: x!=skip)
df=spark.read.csv(odata,header=True, inferSchema=True)
df.printSchema()
df.show(5)
