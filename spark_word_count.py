from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,split,count,col

spark = SparkSession.builder.appName('First').getOrCreate()

lines_df = spark.read.text("x.txt").toDF('line')
test_word = input("Enter word to check:")
word_count = lines_df.select(explode(split('line',' ')).alias('word')).filter(col("word")==test_word)
if(word_count.count()>=1):
    print("exist")
else:
    print("not exist")



