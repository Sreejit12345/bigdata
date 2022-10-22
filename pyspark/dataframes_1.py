from pyspark import SparkConf
from pyspark.sql import SparkSession

spark_conf=SparkConf()
spark_conf.set("spark.app.name","my_pyspark")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

input1=spark.read.format("csv").option("header",True).option("inferSchema",True).option("path","C:/Users/SREEJIT/OneDrive/Desktop/spark practice/orders-201019-002101.csv").load()

df2=input1.select("order_id","order_customer_id").where("order_customer_id>10000").groupBy("order_customer_id").count()


input1.show()
df2.show()

df2.printSchema()


