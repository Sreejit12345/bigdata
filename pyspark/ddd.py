from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import *

"""sc=SparkContext("local[*]","sss")

ratings_input=sc.textFile("E:\\sparkpractice\\ratings-201019-002101.dat")

movies=sc.textFile("E:\\sparkpractice\\movies-201019-002101.dat")

m1=movies.map(lambda x: x.split("::")).map(lambda x: (x[0],x[1]))#(id,name)

rdd1=ratings_input.map(lambda x: x.split("::")).map(lambda x: (x[1],x[2]))
#(movie_id,rating)

rdd2=rdd1.mapValues(lambda x: (int(x),1)) # (movie_id,(rating,1))

rdd3=rdd2.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))# (movie_id, (sum,total)


rdd4=rdd3.filter(lambda x: x[1][1]>1000)
rdd5=rdd4.mapValues(lambda x: x[0]/x[1])
rdd6=rdd5.filter(lambda x: x[1]>4.5)  #(movie_id,avg)


print(rdd6.join(m1).collect())"""


#########################################################
spark_conf=SparkConf()
spark_conf.set("spark.master","local[*]")
spark_conf.set("spark.app.name","sss")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

def read(spark,filepath):
    df=spark.read.format("text").option("sep","::").option("inferSchema","true").option("path",filepath).load()
    return df

ratings=read(spark,"E:\\sparkpractice\\ratings-201019-002101.dat")
movies=read(spark,"E:\\sparkpractice\\movies-201019-002101.dat")

r1=ratings.selectExpr("split(value,'::') sp")
r2=r1.select(expr("sp[1] movie_id"),expr("sp[2] rating"))

m1=movies.selectExpr("split(value,'::') sp")
m2=m1.select(expr("sp[0] movieid"),expr("sp[1] name"))


r3=r2.groupBy("movie_id").agg(expr("avg(rating) avg"),expr("count(*) cnt ")).filter("avg>=4.5").filter("cnt>=1000").drop("avg").drop("cnt")

join_cond=m2.movieid==r3.movie_id

r3.join(m2,join_cond,"inner").drop("movie_id").show()
spark.sql("use default")
spark.sql("show tables").show()








