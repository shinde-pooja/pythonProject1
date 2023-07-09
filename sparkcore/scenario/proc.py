from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc = spark.sparkContext
data="C://Users//sai//Documents//spark-table//maintec_tab//maintec.txt"
# df= spark.read.format('text').option('header', 'true').option('inferschema','true').option('sep','|').load(data)

df = spark.read.text(data)

for i in df.collect():
    print(i)

ndf= df.toDF()
ndf.show()
