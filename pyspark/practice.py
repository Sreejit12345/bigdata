from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark_conf=SparkConf()
spark_conf.set("spark.app.name","my_app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

df1=spark.read.format("csv").option("inferSchema",True).option("path","C:\\Users\\SREEJIT\\OneDrive\\Desktop\\spark practice\\-201125-161348.dataset1").load()

df2=df1.toDF("Name","Age","City")   # To change column names from defualt names

#---------to select in columns---------------------------
df2.select("Name","Age","city").show()

df2.select(col("Name"),col("Age"),col("City")).show()

df2.select(column("Name"),column("Age"),column("City")).show()

#-----------to use column expressions---------------------------


df2.select(col("Name"),expr("concat(Age,City)")).show()

df2.select(col("Name"),expr("concat(Age,City) concatcol")).show()  #use alias

df2.selectExpr("Name","concat(Age,City)  concol").show()      # use alias

#-----------to use aggregates-------------------------------

df2.printSchema()
df2.selectExpr("count(*) CNT","sum(Age) sumage") .show()    #just gives sum of age and count of all columns in dataframe
df2.select(count("*").alias("CNT"),sum("Age").alias("sumage")).show()

df2.select("Age").groupBy("Age").count().show()





