from pyspark import SparkConf
from pyspark.sql import SparkSession

spark_conf=SparkConf()
spark_conf.set("spark.app.name","my_app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

ip=spark.read.format("jdbc").option("url","jdbc:oracle:thin:@localhost:1521:xe")\
.option("dbtable","abcdefgh").option("user","admin").option("password","s2d3s2d3")\
.option("driver","oracle.jdbc.driver.OracleDriver").load()


ip.show()
