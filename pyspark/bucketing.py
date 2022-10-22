from pyspark import SparkConf
from pyspark.sql import SparkSession

spark_conf=SparkConf()
spark_conf.set("spark.app.name","my_app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).enableHiveSupport().getOrCreate()

#df=spark.read.format("csv").option("header",True).option("path","C:/Users/SREEJIT/OneDrive/Desktop/spark practice/orders-201019-002101.csv").load()

#df.createOrReplaceTempView("temp")   


#df_mod=spark.sql("select * from temp where ")  # using spark sql to create a df

spark.sql("select * from pyspark.py_tbl").show(truncate=False)   # create a database inside hive metastore

#df_mod.write.format("csv").mode("overwrite").partitionBy("order_status").bucketBy(5,"order_id").saveAsTable("pyspark.py_tbl")










