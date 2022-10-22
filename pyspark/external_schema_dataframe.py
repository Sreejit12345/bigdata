from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,TimestampType,StringType

spark_conf=SparkConf()
spark_conf.set("spark.app.name","ddl_string")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

DDl_String="o_id Integer,date Timestamp,cust_id Integer,Status String"

input_df=spark.read.format("csv").schema(DDl_String).option("header",True).option("path","C:/Users/SREEJIT/OneDrive/Desktop/spark practice/orders-201019-002101.csv").load()

input_df.printSchema()   # external Schema using DDL string

orders_schema=StructType([StructField("o_id",IntegerType()),StructField("o_date",TimestampType()),StructField("cust_id",IntegerType()),StructField("status",StringType())])


input_df1=spark.read.format("csv").schema(orders_schema).option("header",True).option("path","C:/Users/SREEJIT/OneDrive/Desktop/spark practice/orders-201019-002101.csv").load()

input_df1.printSchema()

