from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("xyz.com").getOrCreate()

data=[("James","","Smith","36636","M",60000),
        ("Michael","Rose","","40288","M",70000),
        ("Robert","","Williams","42114","",400000),
        ("Maria","Anne","Jones","39192","F",500000),
        ("Jen","Mary","Brown","","F",0)]
columns=["first_name","middle_name","last_name","dob","gender","salary"]
df = spark.createDataFrame(data=data,schema=columns)
df.show()
df=df.toPandas()
print(df)