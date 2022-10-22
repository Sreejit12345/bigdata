
from pyspark import SparkContext

def score_calculator(line1):    #this function will compute the score per course,per user   input= (course_id,(no_of_chapters_read_per_user_for_that_course,total_no_of_chapters))
    course_id=line1[0]
    no_of_chapters=float(line1[1][0])
    total_chapters=float(line1[1][1])
    percentage= (no_of_chapters/total_chapters)*100

    if (percentage >=90):
        score=10
    elif(percentage>=50 and percentage<90):
        score=4
    elif(percentage>=25 and percentage<50):
        score=2
    else:
        score=0
    
        
    return (course_id,score)
    

sc=SparkContext("local[*]","week_10_assignment")
sc.setLogLevel("ERROR")
titles=sc.textFile("C:/Users\\SREEJIT\\OneDrive\\Desktop\\spark practice\\titles-201108-004545.csv")
titles1=titles.map(lambda x: x.split(",")).map(lambda x : (int(x[0]),x[1]))# to get (course_id,title)
chapters_input=sc.textFile("C:\\Users\\SREEJIT\\OneDrive\\Desktop\\spark practice\\chapters-201108-004545.csv")  # to load the chapters file
views_input=sc.textFile("C:\\Users\\SREEJIT\\OneDrive\\Desktop\\spark practice\\views1-201108-004545.csv,C:\\Users\\SREEJIT\\OneDrive\\Desktop\\spark practice\\views2-201108-004545.csv,C:\\Users\\SREEJIT\\OneDrive\\Desktop\\spark practice\\views3-201108-004545.csv")
# we give muplitple files in  single comma separated ""
rdd0=chapters_input.map(lambda x: x.split(",")).map(lambda x: (int(x[0]),int(x[1])))  # we get (chapterId,courseId)
rdd1=chapters_input.map(lambda x: x.split(",")).map(lambda x: (int(x[1]),1))   # to split and get  (course_id,1) 

rdd2=rdd1.reduceByKey(lambda x,y : x+y) # we get (course_id,total_no_of_chapters)
views1=views_input.map(lambda x: x.split(",")).map(lambda x: (int(x[1]),int(x[0])))  # remove timestamp and get (chapterId,userId)

rdd3=views1.distinct()
# now we join rdd0 and rdd3 on chapter_id
joined_rdd= rdd0.join(rdd3)  # we get (chapter_id,(courseId,userId))
joined_rdd1=joined_rdd.map(lambda x: ((x[1][1],x[1][0]),x[0])) # we get ((userId,course_id),chapter_id)
# now we can find the no_of chapters read in a course per user by adding up the entries for course_id
joined_rdd2=joined_rdd1.map(lambda x: (x[0],1)) # we get (userId,course_id),1)
summed_rdd=joined_rdd2.reduceByKey(lambda x,y : x+y)  # we get ((userId,course_id),total_chapters_done_per_course_per_user)
# Now we need to join rdd2 and summed_rdd to find out the total_chapters
summed_rdd1=summed_rdd.map(lambda x: (x[0][1],x[1]))  # to get (course_id,total_course_done_per_course_per_user)
rdd_join2=summed_rdd1.join(rdd2)  # we get (course_id,(no_of_chapters_read_per_user_for_that_course,total_no_of_chapters))
#now we need to find out the score per user_id,per_course
rdd_score=rdd_join2.map(score_calculator)   # we get (course_id,score) 
final_rdd=rdd_score.reduceByKey(lambda x,y : x+y) #(course_id,total_score) 

ss=final_rdd.join(titles1) # we get (course_id,(total_score,title)

final_pakka=ss.map(lambda x: (x[1][1],x[1][0])).sortBy(lambda x: x[1],False)
ddd=final_pakka.collect()

for i in ddd:
    print(i)








