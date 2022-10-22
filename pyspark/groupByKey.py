from pyspark.sql import SparkSession
from pyspark import SparkConf

spark_conf=SparkConf()
spark_conf.set("spark.app.name","groupby")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

i_rdd=spark.sparkContext.textFile("C:\\Users\\SREEJIT\\OneDrive\\Desktop\\shared1\\dd.txt")

rdd1=i_rdd.flatMap(lambda x : x.split(" ")).map(lambda x : (x,1))    

rdd2=rdd1.groupByKey()   #[(hello,{1,1,1,1}),(hi,{1,1,1,1})]
rdd3=rdd2.collect()     # we need to collect since groupBykey is a transformation

for x,y in rdd3:
    print(x,len(y))    #print word, length_of 1s in tuple...which eventually gives us the count of each word
input()
