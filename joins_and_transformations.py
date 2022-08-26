from pyspark.sql import SparkSession
import pandas
from pyspark.sql.window import Window
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("xyz.com").getOrCreate()
#1 Compute the number of movies produced each year. The output should have two columns: year and count.
# the output should be ordered by the count in descending order.
movie_df = spark.read.csv("../Spark-practice/resource/movies.csv",header=True,sep="\t",inferSchema=True)
#movie_df.show()
movie_rating_df = spark.read.csv("../Spark-practice/resource/movie_rating.csv",header=True,sep="\t",inferSchema=True)
#movie_rating_df.show()
count_df = movie_df.groupBy("year").agg(count("movie").alias("count")).orderBy(col("count").desc())
#count_df.show()
#Compute the number of movies each actor was in. The output should have two columns: actor and count.
#the output should be ordered by the count in descending order.

actor_df = movie_df.groupBy("actor").agg(count("movie").alias("count")).orderBy(col("count").desc())
#actor_df.show()

windowspec = Window.partitionBy("year").orderBy(col("rating").desc())

first_df = movie_rating_df.withColumn("row_number",dense_rank().over(windowspec))
first_df=first_df.filter(col("row_number")==1)
#first_df.show()

join_df = first_df.join(movie_df,first_df.movie_name==movie_df.movie,"left")
#join_df.show()

join_df_new= movie_df.join(movie_rating_df,movie_df.movie==movie_rating_df.movie_name,"inner")
#join_df_new.show()

new_df = movie_df.alias("movie1").join(movie_df.alias("movie2"),col("movie1.movie") == col("movie2.movie"),"inner").\
    groupBy("movie1.actor").agg(count("movie1.movie").alias("count")).orderBy(col("count").desc())
new_df.show()
# new_df.write.csv("../Spark-practice/output")
