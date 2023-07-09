from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc = spark.sparkContext


# data = "E:\\big-data\\drivers-20220726T155648Z-001\\10000Records.csv"
# df= spark.read.format('csv').option('header','true').option('inferschema','true').load(data)
# import re
# cols=[re.sub('[^a-zA-Z0-9]','',c) for c in df.columns]
# ndf=df.toDF(*cols)
# pdf=ndf.withColumn('DateofBirth',to_date(col('DateofBirth'), 'm/d/yyyy')).withColumn('today', current_date())\
#     .withColumn('age', (datediff(col('today'),col('DateofBirth'))/365.25))\
#     .withColumn('age', round(col('age')))
# pdf.show()

# --------------- to save file to database -------------

# from configparser import ConfigParser
# conf = ConfigParser()
# conf.read(r"E:\\big-data\\files\\config.txt")
# host=conf.get(cred,host)
# host=conf.get(cred,user)

# spark.write.format('jdbc').option('host','hostname').option('url','url')\
#     .option('password','pass').option('dbtable','tablename').option('driver','com.mysql.jdbc.Driver').save()


# ---------------- rdd use case ----- skip header---------------------
#
# data="E:\\big-data\\drivers-20220726T155648Z-001\\donations1.csv"
# df=sc.textFile(data)
# skip= df.first()
# odata=df.filter(lambda x : x != skip)
# ndf = spark.read.csv(odata).option('header','true')
# ndf.show()

# --------------------select and where use case -----------------------------------


# data = "E:\\big-data\\drivers-20220726T155648Z-001\\drivers\\bank-full.csv"
#
# df= spark.read.format('csv').option('header','true').option('inferschema','true').option('sep',';').load(data)
#res=df.where(col('age')>90)
#res = df.where((col('marital')!= 'married') & (col('age')> 60))
#res = df.select(col('age'), col('marital'), col('balance')).where((col('age')> 60) & (col('marital')== 'divorced'))
#res = df.groupBy(col('marital')).agg(sum(col('balance')).alias('sumbal')).orderBy(col('sumbal'))
# select marital count(*), cnt , sum(balance) sum from tab where balance >= avg(balance)
# res= df.groupBy(col('marital')).agg(count('*').alias('cnt')),sum(col('balance')).alias('sum').where(col('balance')>= avg(col('balance')))
#res= df.groupBy(col('marital')).agg(count("*").alias("cnt").sum(col("balance").alias("sum")))#.where(col("balance")>=avg(col("balance")))
# res.show()
# df.createOrReplaceTempView('tab')
#ndf= spark.sql('select * from tab where marital = "married" and balance >= 4000')
#ndf = spark.sql('select marital, sum(balance) as sumbal from tab group by marital')
#ndf.show()

# --------------------- date function--------------------------------


# data = "E:\\big-data\\drivers-20220726T155648Z-001\\donations.csv"
#
# df=spark.read.format("csv").option("header", "true").option('inferSchema', "true").load(data)
# df.show()
# ndf= df.withColumn('dt' , to_date(col('dt'), 'd-M-yyyy'))\
#     .withColumn('curr', current_date())\
#     .withColumn('curr_tsmp', current_timestamp())\
#     .withColumn('datediff', datediff(col('curr'), col('dt')))\
#     .withColumn('adddt', date_add(col('curr'), 100))\
#     .withColumn('subdt', date_sub(col('curr'), 100))\
#     .withColumn('lastdt', last_day(col('curr')))\
#     .withColumn("dttrunc", date_trunc("year", col("dt")))\
#     .withColumn("montruc", date_trunc("mon", col("dt")))\
#     .withColumn("weekofyr", weekofyear(col("dt")))
# ndf.show()


# -------------------- substring use case------------------

data="E:\\big-data\\drivers-20220726T155648Z-001\\drivers\\us-500.csv"

df= spark.read.format('csv').option('header','true').option('inferschema', 'true').option('sep',',').load(data)
#
# ndf= df.withColumn('username', substring(col('email'),0,6))\
# .withColumn('subIndex', substring_index(col('email'),'@',1))\
#     .withColumn('substring', substring_index(col('email'), '@', -1))

# -------------------- collect list and set---------------
#  in list and set are work as same but list can contail duplicate values but set does not
# ndf = df.groupBy(col('state')).agg(count('*').alias('cnt'), collect_list(col('first_name').alias('vacinatted name'))).orderBy(col('cnt'))
# ndf= df.groupBy(col('state')).agg(count("*").alias('cnt'), collect_set(col('first_name').alias('name'))).orderBy(col('cnt'))

# --------------- withcolumn concat_ws, renamed, --------------------------
# spark function :
# lit()
# concat_ws()
# cast(longtype())
# regexp_replace()
# drop(column name)
#
# ndf = df.withColumn('age', lit(18)).withColumn('name', concat_ws(' ', col('first_name'), col('last_name'), col('state')))\
#     .withColumn('phone1', regexp_replace(col('phone1'),'-',''))\
#     .withColumnRenamed('first_name','fname')
#
# ndf.show()
#

# -------------- reduce by key ------------------
#
# data = "E:\\big-data\\drivers-20220726T155648Z-001\\donations.csv"
#
# rdd= sc.textFile(data)
#
# df= rdd.filter(lambda x : 'dt' not in x).map(lambda x : x.split(','))
#
# for i in df.collect():
#     print(i)
#

# ----------------------------- word count program-------------------

data =['pooja', 'nitin','yogesh','pooja','nitin','sachin','mammi','pappa','pappa','pappa']
df=sc.parallelize(data)
for i in df.collect():
    print(i)

# df1=df.flatMap(lambda x: x.split(' ')).map(lambda x: (x,1) ).reduceByKey(lambda x,y: x+y)

df1=df.flatMap(lambda x: x.split(' ')).map(lambda x: (x,1) )

for i in df1.collect():
    print(i)

df2=df1.reduceByKey(lambda x,y: x+y)

for i in df2.collect():
    print(i)

#  ----------------------------------------------

lst=['1','2','1','3','4','3','4','5','4','2','3','4','5','2','3']

ndf=sc.parallelize(lst)
for i in ndf.collect():
    print(i)


ndf1= ndf.flatMap(lambda x: x.split(' ')).map(lambda x: (x,1))
for i in ndf1.collect():
    print(i)
ndf2= ndf1.reduceByKey(lambda x,y: x+y)
for i in ndf2.collect():
    print(i)
