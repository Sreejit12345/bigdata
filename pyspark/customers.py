from pyspark import SparkContext

sc=SparkContext("local[*]","customers_pyspark")

input_rdd=sc.textFile("C:\\Users\\SREEJIT\\OneDrive\\Desktop\\spark practice\\customerorders-201008-180523.csv")

rdd_1=input_rdd.map(lambda x: x.split(","))

rdd2=rdd_1.map(lambda x: (x[0],float(x[2])) ) # tuple of cust_id and order_amt

rdd3=rdd2.reduceByKey(lambda x,y : x+y).sortBy(lambda x : x[1],False)

rdd_final=rdd3.collect()

for i in rdd_final:
    print(i)
            
