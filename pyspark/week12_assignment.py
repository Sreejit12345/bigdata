from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark_conf=SparkConf()
spark_conf.set("spark.app.name","week_12_assignment")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

employee_dataframe=spark.read.format("json").option("path","C:\\Users\\SREEJIT\\OneDrive\\Desktop\\spark practice\\e.json").load()
department_dataframe=spark.read.format("json").option("mode","PERMISSIVE").option("path","C:\\Users\\SREEJIT\\OneDrive\\Desktop\\spark practice\\d.json").load()


edf1=employee_dataframe.withColumnRenamed("deptid","did")

edf1.show(truncate=False)
department_dataframe.show(truncate=False)


join_cond=edf1.did==department_dataframe.deptid

joined=edf1.join(broadcast(department_dataframe),join_cond,"right")

li=[['Sreejit',[12,23,2]],['Arshia',[12,43,2]]]

dd=spark.createDataFrame(li,["Name","test"])


mod=dd.select(col("Name"),expr("posexplode(test)")).drop("pos")


mod.groupBy("Name").agg(expr("collect_list(col) comma")).show()





