from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract

spark_conf=SparkConf()
spark_conf.set("spark.app.name","reise")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

df=spark.read.format("text").option("inferSchema",True).option("path","C:\\Users\\SREEJIT\\OneDrive\\Desktop\\spark practice\\orders_new-201019-002101.csv").load()

regex=r'^(\S+) (\S+)\t(\S+)\,(\S+)'

finaldf=df.select(regexp_extract('value',regex,1).alias("col1"),regexp_extract('value',regex,2).alias("col2")\
                  ,regexp_extract('value',regex,3).alias("col3"),regexp_extract('value',regex,4).alias("col4"))    # so this is a structure df

fianldf1=finaldf.groupBy("col4").count()    # to perfrom actions on structured dataframep

fianldf1.show()
