from pyspark import SparkContext
from pyspark.streaming import *
 

sc=SparkContext("local[2]","ss")
ssc=StreamingContext(sc,2)
lines=ssc.socketTextStream("localhost",9008)

words=lines.flatMap(lambda x: x.split(" "))

trans=words.map(lambda x: (x,1))

final=trans.reduceByKey(lambda x,y: x+y)

final.pprint()
ssc.start()
ssc.awaitTermination()



