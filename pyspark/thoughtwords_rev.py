from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import DateType

def createSparkSession():
    spark_conf=SparkConf()
    spark_conf.set("spark.app.name","thoughtworks")
    spark_conf.set("spark.master","local[*]")

    spark=SparkSession.builder.config(conf=spark_conf).enableHiveSupport().getOrCreate()

    return spark

spark=createSparkSession()
ip_list=[(1,"2013-07-25",11599,"CLOSED"),(2,"2014-07-25",256,"PENDING_PAYMENT"),(3,"2013-07-25",11599,"COMPLETE"),(4,"2019-07-25",8827,"CLOSED")]

schema=["OID","DT","CID","ST"]
df=spark.createDataFrame(ip_list,schema)

ddd=df.withColumn("UNIX_DATE",col("DT").cast(DateType()))

ddd.select(expr("countDistinct(DT)")).show()

                                                                                                       




    
    
    
    
