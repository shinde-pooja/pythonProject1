from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc = spark.sparkContext

data="E:\\big-data\\drivers-20220726T155648Z-001\\us-500.csv"

df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)


# creating our own function ;
def func(st):
    if(st=="NY"):
        return "30% off"
    elif(st=="CA"):
        return "40% off"
    elif(st=="OH"):
        return "50% off"
    else:
        return "500/- off"

# by default spark unable to understand pythin function so convert python/java/scala/function to UDF
# as spark able to understand UDF function

uf = udf(func)

#ndf=df.withColumn("offer", uf(col("state")))




spark.udf.register("offer", uf) # user define function convert to sql function
df.createOrReplaceTempView("tab")

ndf=spark.sql("select * , offer(state) todaysoffer from tab" )



ndf.printSchema()
ndf.show()