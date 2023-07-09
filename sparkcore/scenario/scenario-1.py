from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc = spark.sparkContext
data1='C:\\Users\\sai\\Documents\\spark-table\\tab2.txt'
data='C:\\Users\\sai\\Documents\\spark-table\\tab1.txt'

data2='C:\\Users\\sai\\Documents\\spark-table\\tab3.txt'
df1= spark.read.format('csv').option('header','true').option('sep','\t').option('inferschema','true').load(data)


df2= spark.read.format('csv').option('header','true').option('sep','\t').option('inferschema','true').load(data1)

ndf= spark.read.format('csv').option('header','true').option('sep','\t').option('inferschema','true').load(data2)

df1.show()
df2.show()
# ------------ join -------------------
# types of join allowed in spark:
# INNER JOIN.
# CROSS JOIN.
# LEFT OUTER JOIN.
# RIGHT OUTER JOIN.
# FULL OUTER JOIN.
# LEFT SEMI JOIN.
# LEFT ANTI JOIN.
df3=df1.join(df2, df1.id==df2.id,'left')
df3.show()


df4=df1.join(df2,df1.id==df2.id,'inner')
df4.show()


#  -------------- unoin -------------------
rdf= df1.union(ndf)

#------------- unoinbyname --------------------
rdf= df1.unionByName(ndf,allowMissingColumns=True)
ndf.printSchema()
df1.printSchema()
rdf.show()