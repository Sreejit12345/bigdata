from pyspark import SparkContext

sc=SparkContext("local[*]","Q1")

input=sc.textFile("C:\\Users\\SREEJIT\\OneDrive\\Desktop\\spark practice\\-201125-161348.dataset1")

rdd1=input.map(lambda x: x.split(","))

rdd2=rdd1.map(lambda x: (x[0],x[1],x[2],'Y') if(int(x[1])>18) else (x[0],x[1],x[2],'N'))

rdd3=rdd2.collect()

for i in rdd3:
    print(i)
