def getpercent(line1):
    course_id=line1[0]
    percent=float(line1[1][0])/float(line1[1][1]) *100

    if(percent>=90):
        score=10
    elif(percent>=50 and percent<90):
        score=4
    elif(percent>=25 and percent<50):
        score=2
    else:
        score=0
    return (course_id,score)
    


from pyspark import SparkContext

sc= SparkContext("local[*]","rev_2")

chapters_file=sc.textFile("E:\\spark practice\\chapters-201108-004545.csv")
titles=sc.textFile("C:\\Users\\SREEJIT\\OneDrive\\Desktop\\spark practice\\titles-201108-004545.csv")
titles1=titles.map(lambda x: x.split(",")).map(lambda x : (int(x[0]),x[1]))# to get (course_id,title)

views1=sc.textFile("E:\\spark practice\\views1-201108-004545.csv")
views2=sc.textFile("E:\\spark practice\\views2-201108-004545.csv")
views3=sc.textFile("E:\\spark practice\\views3-201108-004545.csv")

views_total=views_input=sc.textFile("E:\\spark practice\\views1-201108-004545.csv,E:\\spark practice\\views2-201108-004545.csv,C:\\Users\\SREEJIT\\OneDrive\\Desktop\\spark practice\\views3-201108-004545.csv")


chapters_file_11=chapters_file.map(lambda x: x.split(",")).map(lambda x: (x[0],x[1]))   # to get (chapter_id,course_id)


chapters_file_1=chapters_file.map(lambda x: x.split(",")).map(lambda x: (x[1]))

dict1=chapters_file_1.countByValue()   # this holds the course_id and the correspondind count of chapters

rdd0=chapters_file.map(lambda x: x.split(",")).map(lambda x: (int(x[0]),int(x[1])))  # we get (chapterId,courseId)


rdd1=chapters_file.map(lambda x: x.split(",")).map(lambda x: (int(x[1]),1))   # to split and get  (course_id,1) 

rdd2=rdd1.reduceByKey(lambda x,y : x+y) # we get (course_id,total_no_of_chapters)

views_mod=views_total.map(lambda x: x.split(",")).map(lambda x : (x[1],x[0])).distinct()  # to just get (chapter_id and user_id)



chapter_joined=chapters_file_11.join(views_mod)




chap=chapter_joined.map(lambda x: ((x[1][1],x[1][0]),1))  # we get ((user_id,course_id),1)
chap1=chap.reduceByKey(lambda x,y : x+y)   # we get (user_id,course_id),number_od courses dne per course per user

chap11=chap1.map(lambda x: (int(x[0][1]),x[1]))  # get (course_id,number_od courses dne per course per user)

jd=chap11.join(rdd2)  # (course_id,(number_od chaps dne per course per user,total_chaps))

score=jd.map(getpercent)  # we get (course_id,score)  per user
sc1=score.reduceByKey(lambda x,y: x+y)  # (course_id,total_score)

final_join=sc1.join(titles1).map(lambda x : (x[1][1],x[1][0])).sortBy(lambda x: x[1],False).collect()
print(final_join)

 


