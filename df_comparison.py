from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Data-comparator").getOrCreate()


# get the data from files
def get_df(file_path):
    df = spark.read.csv(file_path, header=True)
    df = df.cache()
    return df


def count_records(df):
    return df.count()


# get the difference in count
def difference_in_count(source, target):
    difference = abs(source.count() - target.count())
    print(f"Difference in count : {difference}")


# missing records in target
def missing_in_target(df1, df2):
    result_df = df1.subtract(df2)
    print(f"{result_df.count()} record missing in target")
    return result_df


def missing_in_source(df1, df2):
    result_df = df2.subtract(df1)
    print(f"{result_df.count()} record missing in source")
    return result_df


def matching_records(df1, df2):
    result_df = df1.intersect(df2)
    print(f"{result_df.count()} records matching")
    return result_df


# get source dataframe
source_df = get_df('../Spark-practice/resource/emp.csv')
source_df.show()
# get target dataframe
target_df = get_df('../Spark-practice/resource/emp_migrated.csv')
target_df.show()
# calculate difference in count
difference_in_count(source_df, target_df)

result_df = missing_in_target(source_df, target_df)
result_df.show()
result_df = missing_in_source(source_df, target_df)
result_df.show()
result_df = matching_records(source_df, target_df)
result_df.show()
