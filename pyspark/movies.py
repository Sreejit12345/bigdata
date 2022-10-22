from pyspark import SparkContext

sc=SparkContext("local[*]","movies_pyspark")

input_rdd=sc.textFile("C:\\Users\\SREEJIT\\OneDrive\\Desktop\\spark practice\\moviedata-201008-180523.data")

rdd1=input_rdd.map(lambda x : x.split("\t"))

rdd2=rdd1.map(lambda x: x[2]) # we get rating,movie_id as a tuple

rdd_final=rdd2.countByValue()


for ele in rdd_final:
    print("Movies rated",ele,"star occured",rdd_final[ele],"times")
    


                
    
