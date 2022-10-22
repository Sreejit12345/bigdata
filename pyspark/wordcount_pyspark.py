from pyspark import SparkContext

if(__name__=="__main__"):   #just to make sure code is being run directly
    
    
    
    sc = SparkContext("local[*]","wordcount_pyspark")
    sc.setLogLevel("ERROR")    #to see error messages 
    sc.setLogLevel("INFO")    #to see info messages

    ip = sc.textFile("C:\\Users\\SREEJIT\\OneDrive\\Desktop\\spark practice\\search_data.txt")

    rdd1=ip.flatMap(lambda x: x.split(" "))  # convert in array of words   rdd1 is RDD[String]

    rdd2=rdd1.map(lambda x: (x.lower(),1))# convert to tuple in (word,1) form, also convert tp lowercase   rdd2= RDD[(String,Int)]

    rdd3=rdd2.reduceByKey(lambda x,y: x+y)
    rdd4=rdd3.sortBy(lambda x :x[1],False)  #using sortBy

    final_rdd=rdd4.collect()
    

    

    for ele in final_rdd:
        print(ele)
    #this is just to keep program running so we can see dag
else:

    print("Not running directly, has been imorted somehwere")
