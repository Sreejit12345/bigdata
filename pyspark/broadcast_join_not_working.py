from pyspark import SparkContext

#-----------------------This function is used to load file from local and return a list of tuples [('1', 'Toy Story (1995)'),('2', 'Jumanji (1995)')]---------------------
def load_movies_file():
    

    file=open("C:/Users/SREEJIT/OneDrive/Desktop/spark practice/movies-201019-002101.dat","r")
    initial_list=file.readlines()   # this loads all the lines in the file in a list of lines
    final_list=[]

    for i in initial_list:
        new_line=i.replace("\n","")
        a1=new_line.split("::")
        tup=(a1[0],a1[1])
        final_list.append(tup)
    return final_list


sc=SparkContext("local[*]","broadcast_join")
        
ratings_input=sc.textFile("C:/Users/SREEJIT/OneDrive/Desktop/spark practice/ratings-201019-002101.dat")

broad_var=sc.broadcast(load_movies_file())  #to broadcast local list into all the nodes of cluster

rdd1=ratings_input.map(lambda x: x.split("::")).map(lambda x :(x[1],float(x[2])))  # we get (movie_id,rating)


rdd2=rdd1.mapValues(lambda x: (x,1))  # we get (movie_id,(rating,1))

rdd3=rdd2.reduceByKey(lambda x,y : (x[0]+y[0], x[1]+y[1])).filter(lambda x: x[1][1]>1000)  # per movie_id we get (movie_id,(sum_of_ratings,no_of_ratings))   added code to filter movies
#with less than 1000 views


rdd4=rdd3.mapValues(lambda x: (x[0]/x[1]))  # we get (movie_id,average_rating)

rdd5=rdd4.filter(lambda x: x[1]>4.5)  #to filter movies whose average <4.5
#print(broad_var.value[0])

rdd6=rdd5.filter(lambda x: x[0]==broadcast(broad_var))

print(rdd6.collect())








