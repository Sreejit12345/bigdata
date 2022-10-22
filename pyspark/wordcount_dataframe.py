from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("word_count").master("local[*]").getOrCreate()

df=spark.read.text("C:\\Users\\SREEJIT\\OneDrive\\Desktop\\dd.txt")  # Sreejit Is good boy

df_array_format=df.select(split(col("value"),' ').alias('words'))  #convert string of column to array [Sreejit,Is,good,boy]
r=ddf.selectExpr("explode(words) words1") # explode will convert array to multiple rows
op=r.groupBy("words1").agg(count("*").alias("CNT"))
op.show(truncate=False)

