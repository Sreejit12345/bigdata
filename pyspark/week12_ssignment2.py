from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark_conf=SparkConf()
spark_conf.set("spark.app.name","df_approach")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()
#-------------------------approach using RDD---------------------------------------------------------------------------------------------
movie_rdd=spark.sparkContext.textFile("C:\\Users\\SREEJIT\\OneDrive\\Desktop\\spark practice\\ratings-201019-002101.dat")

title_rdd=spark.sparkContext.textFile("C:\\Users\\SREEJIT\\OneDrive\\Desktop\\spark practice\\movies-201019-002101.dat")
rdd2=movie_rdd.map(lambda x: x.split("::")).map(lambda x: (x[1],int(x[2])))  #(movie_id,rating)
rdd3=rdd2.mapValues(lambda x: (x,1))   #(movie_id,(rating,1)
rdd4=rdd3.reduceByKey(lambda x,y : (x[0]+y[0],x[1]+y[1]))   #movie_id,(sum_of_rating,no_of_movies)
rdd5=rdd4.mapValues(lambda x: x[0]/x[1])  #movie_id,average

rdd_joined=rdd5.join(rdd4)   #(movie_id,(average,(sum_of_rating,no_of_movies)))

final=rdd_joined.map(lambda x: (x[0],x[1][0],x[1][1][1]))  # (movie_id,average,no_of_movies)
final_filter=final.filter(lambda x: (x[1]>=4.5 and x[2]>1000))

dd=title_rdd.map(lambda x: x.split("::")).map(lambda x: (x[0], x[1]))  # (movie_id,movie_name)
ss=final_filter.join(dd).map(lambda x : (x[1][1],x[0])).sortBy(lambda x: x[1],False).collect()
                          
print(ss) 

#------------------approach using structured APIs----------------------------------------------------------------------------------------------

movie_rdd=spark.sparkContext.textFile("C:\\Users\\SREEJIT\\OneDrive\\Desktop\\spark practice\\ratings-201019-002101.dat")
title_rdd=spark.sparkContext.textFile("C:\\Users\\SREEJIT\\OneDrive\\Desktop\\spark practice\\movies-201019-002101.dat")

we=title_rdd.map(lambda x: x.split("::")).map(lambda x: (x[0],x[1])).toDF(['movie_id','movie_name'])

s=movie_rdd.map(lambda x: x.split("::")).map(lambda x: (x[1],int(x[2]))).toDF(["movie_id","rating"])  #convert to a dataframe with 2 columns


#s.groupBy('movie_id').agg(expr("count(*) as CNT"),expr("avg(rating) as rt")).show()    # same as below

grouped_data=s.groupBy('movie_id').agg(count("*").alias("CNT"),avg("rating").alias("RT"))
gg=grouped_data.select("movie_id").where("CNT>1000 and RT>=4.5")  # filter records

final_join=we.join(gg,gg.movie_id==we.movie_id,"inner").drop(gg.movie_id).show(truncate=False)  # joinig to get titles and then dropping redundant rows"""

