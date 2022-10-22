from pyspark import SparkContext

sc=SparkContext("local[*]","Q2")

input1=sc.textFile("C:\\Users\\SREEJIT\\OneDrive\\Desktop\\spark practice\\tempdata-201125-161349.csv")

rdd1=input1.map(lambda x: x.split(",")).map(lambda x: (x[0],float(x[3])))  # convert to (station_id,temp) format

rdd2=rdd1.reduceByKey(lambda x,y : x if(x<y) else y)  #reduceByKey takes 2 inputs and works on the value, so we can directly calculate min 

rdd3=rdd2.collect()

for i in rdd3:
    print(i)

