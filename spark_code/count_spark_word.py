'''
1. We Need to calculate how many times spark comes under Akshay and how many times spark comes under Ajay.

Txt file data:

Akshay: Spark is a data processing engine that is very fast because of its in-memory processing. The current version of spark is 3.0.0. This version of spark supports the delta lake feature which helps the CDC concept.
Ajay: Apache Spark is a multi-language engine for executing data engineering.
Akshay: Spark is a Big Data Engine.

Output:
Akshay:4
Ajay:1

'''


from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import col,split,lit,count,explode,lower

spark = SparkSession.builder.appName('First').getOrCreate()
file = "../resource/input.txt"
df = spark.read.text(file)
df.printSchema()
df_split = df.select(f.split(df.value,":")).rdd.flatMap(lambda x:x).toDF(schema=["col1","col2"])
df_split.show(truncate=False)
new_df = df_split.select("col1",split(col("col2")," ").alias("textarray"))
new_df.show(truncate=False)
new_df.printSchema()
df = new_df.select("*",explode("textarray").alias("exploded"))\
    .where(lower(col("exploded"))=="spark")\
    .groupBy("col1","textarray")\
    .agg(count("exploded").alias("sparks"))\
    .drop("textarray")
df.show()
df.printSchema()
df.groupBy("col1").sum("sparks").show()