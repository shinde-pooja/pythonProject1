from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()

# data =[12,32,34,4,54,26]
# drdd=spark.sparkContext.parallelize(data)


data = "E:\\big-data\\drivers-20220726T155648Z-001\\drivers\\asl.csv"
aslrdd = spark.sparkContext.textFile(data)

res = aslrdd.filter(lambda x: 'age' not in x).map(lambda x: x.split(",")).toDF(["name", "age", "city"])
res.show()

res.createOrReplaceTempView('tab')
result= spark.sql("select * from tab where city='blr' and age<30")
result.show()

result1 = res.where(col('age')>=30)
result1.show()


result2= res.where((col("age")>=30) & (col('city')=='hyd'))
result2.show()
