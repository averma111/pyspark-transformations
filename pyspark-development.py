# Databricks notebook source
from pyspark import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

def to_Date_Df(df , fmt , field):
  return df.withColumn(field ,to_date(col(field),fmt))

# COMMAND ----------

from pyspark.sql import Row

myschema = StructType([
  StructField("Id",StringType()),
  StructField("EventDate",StringType())
])
myrows = [Row("101","4/5/2020"),Row("102","8/9/2020"),Row("103","3/5/2020"),Row("104","9/10/2020")]
myrdd = spark.sparkContext.parallelize(myrows , 2)
mydf = spark.createDataFrame(myrdd,myschema)

# COMMAND ----------

mydf.printSchema()
mydf.show()

mynewdf = to_Date_Df(mydf,"M/d/y","EventDate")

mynewdf.printSchema()
mynewdf.show()

# COMMAND ----------


