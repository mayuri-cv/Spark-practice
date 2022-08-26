from pyspark.sql import SparkSession
from pyspark.sql.functions import *
# columns = ["eid","ename","loc1","loc2","loc3"]
# val =[(1,"Vasu","AP","Blr", "Pune"),(2,"Sailu","AP","Blr","NULL"),(3,"Kasi","TS","Pune","NULL")]
spark = SparkSession.builder.appName('create df').getOrCreate()
# df = spark.createDataFrame(data=val,schema=columns)
# new_df = df.withColumn("loc",concat_ws(",","loc1","loc2","loc3")).select("eid","ename",explode(split("loc",",")))
# new_df.show()
# # new_df = new_df.select("eid","ename",split("loc",",").alias('loc_col'))
# # new_df.show()
# #new_df.select("eid","ename",explode("loc_col")).show()

####################################################################
columns = ["empid","empname","managerid","salary","joining_date","gender"]
val = [(1,"Ram",4,100,"02-11-2021","M"),
(2,"Raj",3,80,"02-11-2021","M"),
(3,"Shehal",4,120,"02-01-2001","F"),
(4,"Saiee",5,150,"02-01-2002","F"),
(5,"Rakesh",None,180,"02-01-2003","M"),
(6,"Rachna",4,110,"02-01-2006","F")]
df = spark.createDataFrame(data=val,schema=columns)
#df.show()

# df = df.select("empid","empname","salary",to_date(col("joining_date"),"MM-dd-yyyy").alias("joining_date"))
# #df.printSchema()
# new_df = df.select("empid","empname","salary",year("joining_date").alias('year')).filter("year>2005 and gender='M'")
# new_df.show()
df.withColumn("year", df.joining_date.substr(-4, 4).cast("int")). \
    filter("year > 2005 and gender = 'M'").show()
