from pyspark.sql import SparkSession
from pyspark import SparkConf

spark_conf=SparkConf()
spark_conf.set("spark.app.name","sql")
spark_conf.set("spark.master","local[*]")
spark_conf.set("spark.sql.warehouse.dir","C:\\Users\\SREEJIT\\OneDrive\\Desktop")

spark=SparkSession.builder.config(conf=spark_conf).enableHiveSupport().getOrCreate()

spark.sql("create database if not exists sreejit")
spark.sql("use sreejit")
spark.sql(""" create table if not exists nyse_eod (
col1 string,
col2 bigint,
col3 double,
col4 double,
col5 double,
col6 double,
col7 bigint)
row format delimited fields terminated by ','
stored as textfile""")

path='"C:/Users/SREEJIT/OneDrive/Desktop/jh"'
#spark.sql(f"load data local inpath {path} overwrite into table nyse_eod")

#spark.sql("select * from nyse_eod limit 5").show(truncate=False)

spark.read.table("sreejit.nyse_eod").show(truncate=False)

