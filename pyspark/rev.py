from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkConf

def createSession():
    spark_conf=SparkConf()
    spark_conf.set("spark.app.name","rev")
    spark_conf.set("master","local[*]")
    spark=SparkSession.builder.config(conf=spark_conf).enableHiveSupport().getOrCreate()
    return spark

    

    
    




    
    
    
    
    

    
    
    

    
    
    
    
    
