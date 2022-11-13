from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc = spark.sparkContext

data="E:\\big-data\\datasets\\datasets\\world_bank.json"

df=spark.read.format("json").load(data)

"""
ndf=df.withColumn("majarsector_percent", explode(col("majorsector_percent"))).withColumn("majarsector_percent_name", col("majorsector_percent.Name")).withColumn("majarsector_percent_percent",col("majorsector_percent.Percent"))\
    .withColumn("mjsector_namecode",explode(col("mjsector_namecode"))).withColumn("mjsector_namecode_code",col("mjsector_namecode.code"))\
    .withColumn("mjsector_namecode_name", col("mjsector_namecode.name")).withColumn("mjtheme_namecode",explode(col("mjtheme_namecode")))\
    .withColumn("mjtheme_namecode_code",col("mjtheme_namecode.code")).withColumn("mjtheme_namecode_name",col("mjtheme_namecode.name"))\
    .withColumn("projectdocs",explode(col("projectdocs"))).withColumn("projectdocs_DocDate",col("projectdocs.DocDate")).withColumn("projectdocs_docType",col("projectdocs.DocType"))\
    .withColumn("projectdocs_DocTypeDesc",col("projectdocs.DocTypeDesc")).withColumn("projectdocs_Docurl", col("projectdocs.DocURL"))\
    .withColumn("sector", explode(col("sector"))).withColumn("sector_Name", col("sector.Name"))\
    .withColumn("sector_namecode", explode("sector_namecode")).withColumn("sector_namecode_Code", col("sector_namecode.Code"))\
    .withColumn("sector_namecode_name", col("sector_namecode.name"))\
    .withColumn("theme_namecode", explode("theme_namecode")).withColumn("theme_namecode_Code", col("theme_namecode.Code"))\
    .withColumn("theme_namecode_name", col("theme_namecode.name"))\
    .withColumn("sector1_Name", col("sector1.Name")).withColumn("sector1_pecent",col("sector1.Percent"))\
    .withColumn("sector2_Name", col("sector2.Name")).withColumn("sector2_pecent",col("sector2.Percent"))\
    .withColumn("sector3_Name", col("sector3.Name")).withColumn("sector3_pecent",col("sector3.Percent"))\
    .withColumn("sector4_Name", col("sector4.Name")).withColumn("sector4_pecent",col("sector4.Percent"))\
    .withColumn("theme1_Name", col("theme1.Name")).withColumn("theme1_pecent",col("theme1.Percent")) \
    .withColumn("project_abstract", col("project_abstract.cdata"))\
    .withColumn("id", col("_id.$oid")) \
    .withColumn("majarsector_percent_Name", col("majarsector_percent.Name")).withColumn("majarsector_percent_pecent", col("majarsector_percent.Percent")) \
    .drop("majorsector_percent", "mjsector_namecode","mjtheme_namecode","projectdocs", "sector", "sector1","sector2", "sector3","sector4",\
          "theme1","sector_namecode","project_abstract","theme_namecode","majarsector_percent","_id")

"""
ndf=df.withColumn("majorsector_percent", explode(col("majorsector_percent")))\
    .withColumn("mjsector_namecode",explode(col("mjsector_namecode")))\
    .withColumn("mjtheme_namecode",explode(col("mjtheme_namecode")))\
    .withColumn("mjtheme",explode(col("mjtheme")))\
    .withColumn("majorsector_percent_name", col("majorsector_percent.Name"))\
    .withColumn("majorsector_percent_percent",col("majorsector_percent.Percent"))\
    .withColumn("mjsector_namecode_code", col("mjsector_namecode.code"))\
    .withColumn("mjsector_namecode_name",col("mjsector_namecode.name"))\
    .withColumn("project_abstract_cdata",col("project_abstract.cdata"))\
    .drop("majorsector_percent","mjsector_namecode","mjtheme_namecode","project_abstract")\
    .withColumn("projectdocs",explode(col("projectdocs")))\
    .withColumn("sector",explode(col("sector")))\
    .withColumn("sector_namecode", explode(col("sector_namecode")))\
    .withColumn("theme_namecode",explode(col("theme_namecode")))\
    .withColumn("theme_namecode_code",col("theme_namecode.code"))\
    .withColumn("theme_namecode_name",col("theme_namecode.name"))\
    .drop("theme_namecode")\
    .withColumn("theme1_name",col("theme1.Name"))\
    .withColumn("theme1_Percent",col("theme1.Percent"))\
    .drop("theme1")\
    .withColumn("sector_namecode_code",col("sector_namecode.code"))\
    .withColumn("sector_namecode_name",col("sector_namecode.name"))\
    .drop("sector_namecode")\
    .withColumn("sector4_Name",col("sector4.Name"))\
    .withColumn("sector4_Percent",col("sector4.Percent"))\
    .drop("sector4") \
    .withColumn("sector3_Name", col("sector3.Name")) \
    .withColumn("sector3_Percent", col("sector3.Percent")) \
    .drop("sector3") \
    .withColumn("sector2_Name", col("sector2.Name")) \
    .withColumn("sector2_Percent", col("sector2.Percent")) \
    .drop("sector2") \
    .withColumn("sector1_Name", col("sector1.Name")) \
    .withColumn("sector1_Percent", col("sector1.Percent")) \
    .drop("sector1") \
    .withColumn("sector_name",col("sector.Name")).drop("sector")\
    .withColumn("projectdocs_docdate",col("projectdocs.DocDate"))\
    .drop("projectdocs")\
    .withColumn("idoid",col("_id.$oid")).drop("_id")

'''
# explode : if u have any array format remove arrays to remove array use explode
# e.g. 
|-- majorsector_percent: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- Name: string (nullable = true)
 |    |    |-- Percent: long (nullable = true)

above data remove array ... use explode at that time u ll get like this
#majorsector_percent: struct (nullable = true)
# |    |-- Name: string (nullable = true)
# |    |-- Percent: long (nullable = true)
-----------------------------------------------------------
# to remove STRUCT value:
parent_col.child_column
|-- sector2: struct (nullable = true)
 |    |-- Name: string (nullable = true)
 |    |-- Percent: long (nullable = true)
 
this data convert to theme_namecode_name and theme_namecode_code in this
col("theme_namecode.code"), col("theme_namecode.name") .drop("theme_namecode")

'''
ndf.createOrReplaceTempView("tab")
#res=ndf.where(col("countrycode")!="ET")
res=ndf.groupBy(col("countrycode")).count().orderBy(col("count").desc())
res.show()
res.printSchema()