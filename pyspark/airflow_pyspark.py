from pyspark import SparkContext
import sys
import os

sc=SparkContext("local[*]","airflow_test")
  
#i_rdd=sc.textFile("E:\\spark practice\\orders-201019-002101.csv")
i_rdd=sc.textFile(sys.argv[0])
  
op=i_rdd.map(lambda x: x.split(",")).map(lambda x : (x[0],x[1],x[2],x[3]))
op1=op.filter(lambda x :x[3]=="CLOSED").map(lambda x : x[0]+","+x[1]+","+x[2]+","+x[3])
  
op1.saveAsTextFile(sys.argv[1])

  
