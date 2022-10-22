from pyspark import SparkConf
from pyspark.sql import SparkSession



import random

def randomgen():
    return (random.randrange(1,60))

def getproper(x):

    if(x[0][0:4] =='WARN'):
        return ('WARN',len(x[1]))
    else:
        return ('ERROR',len(x[1]))


spark_conf=SparkConf()
spark_conf.set("spark.app.name","salting")
spark_conf.set("spark.master","local[*]")

spark= SparkSession.builder.config(conf=spark_conf).getOrCreate()

input_rdd=spark.sparkContext.textFile("E:\\bigLogNewtxt-201031-115759\\bigLogNew.txt")

rdd1=input_rdd.map(lambda x: (x.split(":")[0]+str(randomgen()),x.split(":")[1]))  #appending random number



rdd2=rdd1.groupByKey()   # key,{val1,val2,val3}   view just like after shuffle and sort



rdd3=rdd2.map(getproper)

rdd4=rdd3.reduceByKey(lambda x,y: x+y)

rdd5=rdd4.collect()

print(rdd5)





            
