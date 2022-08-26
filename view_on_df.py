from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
      .master("local[2]") \
      .appName("SparkByExamples.com") \
      .getOrCreate()
print(spark)

data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns=["first_name","middle_name","last_name","dob","gender","salary"]
df=spark.createDataFrame(data=data, schema=columns)
df.show()

df = spark.read.csv("source.csv",header=True)
#df.printSchema()
#df.show(200)
df.createOrReplaceTempView("emp_data")
df2 = spark.sql("SELECT * FROM emp_data")
#df2.printSchema()
df2.show()
group_df = spark.sql("SELECT job_id,count(1) FROM emp_data group by job_id")
group_df.show()