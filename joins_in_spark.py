from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
import pandas
spark = SparkSession.builder.appName("xyz.com").getOrCreate()
employee = spark.read.csv("../Spark-practice/resource/employee.csv",header=True,sep=",",inferSchema=True)
department = spark.read.csv("../Spark-practice/resource/department.csv",header=True,sep=",",inferSchema=True)

Df_join = employee.join(department,employee.dept_no == department.id,"inner")
Df_join.show()
Df_join.groupBy("dept_no").count().show()
employee.join(department,employee.dept_no == department.id,"inner").select("first_name","dept_no").show()

employee.join(department,employee.dept_no == department.id,"leftouter").show()
employee.join(department,employee.dept_no == department.id,"rightouter").show()
employee.join(department,employee.dept_no == department.id,"outer").show()
employee.join(department,employee.dept_no == department.id,"leftanti").show()
employee.join(department,employee.dept_no == department.id,"leftsemi").show()
employee.crossJoin(department).show();