from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

print('Storing random numbers in a hive table')
spark.range(100).write.mode("overwrite").parquet("gs://persuasive-net-447201-h6/random_numbers")
print('complete')
