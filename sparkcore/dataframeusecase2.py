from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc = spark.sparkContext

data = "E:\\big-data\\drivers-20220726T155648Z-001\\drivers\\bank-full.csv"

df=spark.read.format("csv").option("header", "true").option("inferschema","true").option("sep",";").load(data)
# sep option used to specify delimiter
# by deault every field consider as string but i want to change columns appropriate datatype like , int, double, string so use inferschema
# if we not mentioned then it will consider it as string
# so if we add to numeric column then it will be concat


# data processing: programming friendly
# select * from tab where age> 90
#res=df.where(col("age")>90)

# select * from table where marital != "marrried" and age > 60
#res= df.where((col("age")>60) & (col("marital")!="married"))

# select age, mariatl, balance from table where age > 60 and marital != "married"
#res= df.select(col("age"),col("marital"),col("balance")).where((col("age")>60) & (col("marital")!="married"))

# selct age, marital, balance from table where age> 60 and marital != married and balance >= 40000
#res= df.select(col("age")>60, col("marital"), col("balance")).where((col("age")> 60) & (col("marital")!= "married") & (col("balance")>=40000))


# process sql friendly
df.createOrReplaceTempView("tab")
#createOrReplaceTempView : = register this dataframe as a table. it is very easy to run sql queries
#res=spark.sql("select * from tab where age> 50 and balance> 50000")
#res=df.where((col("age")>50) & (col("balance")>50000))

# married , how much balnace they have
# divorced , how much balance they have

#res=spark.sql("select marital,sum(balance) sumbal from tab where group by marital ")
#res = df.groupBy(col("marital")).agg(sum(col("balance")).alias("sumbal")).orderBy("sumbal")

#res=df.groupBy(col("marital")).count()
# select marital count(*), cnt , sum(balance) sum from tab where balance >= avg(balance)
res=df.groupBy(col('marital')).agg(count("*").alias("cnt"),sum(col("balance").alias("sum")))#.where(col("balance")>=avg(col("balance")))


res.show()
res.printSchema()