from pyspark import SparkContext
sc=SparkContext("local[*]","friends_pyspark")

ip=sc.textFile("C:\\Users\\SREEJIT\\OneDrive\\Desktop\\spark practice\\friendsdata-201008-180523.csv")

base_rdd=ip.map(lambda x: x.split("::")).map(lambda x: (x[2],float(x[3]))) #we get a tuple of (age,connections)

rdd1=base_rdd.mapValues(lambda x: (x,1))

rdd2=rdd1.reduceByKey(lambda x,y : (x[0]+y[0],x[1]+y[1]))

rdd3=rdd2.mapValues(lambda x: x[0]/x[1]).collect()

for x,y in rdd3:
    print(x,y)
    






