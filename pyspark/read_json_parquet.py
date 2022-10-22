def open_file():

    file=open("C:\\Users\\SREEJIT\\OneDrive\\Desktop\\d.txt","r")
    str1=file.readlines()
    new_list=[]
    for i in str1:
        strrr=i.replace("\n","")
        new_list.append(strrr)
    return list(set(new_list))




from pyspark import SparkContext

sc=SparkContext("local[*]","aa")


ip=sc.textFile("C:\\Users\\SREEJIT\\OneDrive\\Desktop\\dd.txt")


rdd1=ip.flatMap(lambda x: x.split(" "))
rdd6=rdd1.map(lambda x: (x,1))



rdd3=rdd6.groupByKey()
rdd7=rdd3.map(lambda x: (x[0],len(x[1])))

print(rdd7.collect())
              

