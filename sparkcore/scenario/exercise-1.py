from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc = spark.sparkContext
salesdata='C:\\Users\\sai\\Documents\\spark-table\\sales.csv'
proddata='C:\\Users\\sai\\Documents\\spark-table\\products.csv'
sellerdata='C:\\Users\\sai\\Documents\\spark-table\\sellers.csv'

sal= spark.read.format('csv').option('header','true').option('inferschema','true').load(salesdata)
prod= spark.read.format('csv').option('header','true').option('inferschema','true').load(proddata)
selr= spark.read.format('csv').option('header','true').option('inferschema','true').load(sellerdata)

sal.show()
prod.show()
selr.show()

# Find out how many orders, how many products and how many sellers are in the data.

# sal1=sal.agg(count(col('order_id')))
# sal1.show()
# prod1=prod.agg(count(col('product_id')))
# prod1.show()

# salr1=selr.agg(count(col('seller_id')))
# salr1.show()

# How many products have been sold at least once?
# sal.agg(countDistinct(col('product_id'))).show()

# Which is the product contained in more orders?

# sal.groupBy(col('product_id')).agg(count('*').alias('cnt')).orderBy('cnt').limit(1).show()

from pyspark.sql.window import Window

# how many distinct products have been sold in each date
# sal.groupBy(col('date')).agg(countDistinct(col('product_id'))).show()

# What is the average revenue of the orders?

df1=sal.join(prod,prod.product_id==sal.product_id,'left').drop(prod.product_id)

df2=df1.withColumn('revenue',df1.price * df1.num_pieces_sold)
df2.show()

df3=df2.groupBy(year(col('date'))).agg(sum(col('revenue')))
df3.show()





