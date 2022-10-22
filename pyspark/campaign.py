from pyspark import SparkContext

def boringwords():
    file=open("C:\\Users\\SREEJIT\\OneDrive\\Desktop\\spark practice\\boring_words.txt","r")
    read=file.readlines()  #to read the files as a list, with each line(word) as an element
    newlist=[] #to store the words after replacing newlines with blank
    for word in read:
        new_word=word.replace("\n","")  #replacing newlines with blank
        newlist.append(new_word)  #appending to newlist
    converted_read=list(dict.fromkeys(newlist))  #removing duplicates by converting list values to dictionary keys, then converting back to list
    return converted_read   #returning deduplicated boring word list
    
sc=SparkContext("local[*]","campaign")
broadcasted_list=sc.broadcast(boringwords()) #broadcast list to all nodes in cluster
input=sc.textFile("C:\\Users\\SREEJIT\\OneDrive\\Desktop\\spark practice\\bigdatacampaigndata-201014-183159.csv")    #array('line1','line2','line3)

rdd1=input.map(lambda x: x.split(",")).map(lambda x: (float (x[10]),x[0]))  #array(array(395,big data),array(234,big data is huge))
rdd2=rdd1.flatMapValues(lambda x: x.split(" "))

# before flatmaparray(array(395,array(big,data)),array(234,array(big,data,is,huge)))---
#flatMapValues will give---- array((395,big),(395,data),(234,big),(234,data),(234,is),(234,huge))
#flatMapValues works on the values and returns a k,v pair

rdd3=rdd2.map(lambda x: (x[1],x[0])).filter(lambda x: not(x[0] in broadcasted_list.value))
rdd4=rdd3.reduceByKey(lambda x,y: x+y).sortBy(lambda x : x[1],False)
rdd5=rdd4.take(20)  #take is an action . Take 20 means only take first 20 elements from RDD
print(rdd5)

    



