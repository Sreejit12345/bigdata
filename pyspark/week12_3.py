from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark_conf=SparkConf()
spark_conf.set("spark.app.name","week12_3")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

list1=[(1,"Rohit Sharma","India",200,100.2),(1,"Virat Kohli","India",100,98.02),(1,"Steven Smith","Aus",77,77.93)\
      ,(35,"Clive Llyod","WI",29,37.00),(243,"Rohit Sharma","India",23,150.00),(243,"Faf du Plesis","SA",17,35.06)]

df=spark.createDataFrame(list1).toDF("matchnum","batsman","team","runs","strikerate")  #here we dont use a list since data is already structured

#df.select("batsman").groupBy("batsman").avg("runs").show()

adf=df.groupBy("batsman").agg(avg("runs").alias("AVERAGE")).sort(col("AVERAGE").desc())

list2=[("Rohit_Sharma","India"),("Steven_Smith","Aus"),("Virat_Kohli","India")]

df1=spark.createDataFrame(list2).toDF("Batsman","Team")

df2=df1.withColumn("New_Names",regexp_replace("Batsman","_"," "))   # to create a new column with _ replaced by space

dd=df2.drop("Batsman")# drop old column with _

dd.join(adf,dd.New_Names==adf.batsman,"inner").show(truncate=False)# join to see final outpu?t0
       
