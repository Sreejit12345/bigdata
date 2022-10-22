from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *



def generate_year(start_year,end_year):     #generic function to generate list of years
    year_list=[]
    for i in range(int(start_year),int(end_year)+1):
        year_list.append(i)
    return year_list

def generate_generic(year_list):                  # function to zip year and months except for last year
    generic_list=[1,2,3,4,5,6,7,8,9,10,11,12]
    new_list=[]
    for i in range(0,len(year_list)-1):       #[2017,2018,2019]
        # to iterate over each year
        curr_year=year_list[i]        # set current year
        for i in range(0,len(generic_list)):
            
            
        # iterate over months for whole years
            sub_list=[[curr_year,generic_list[i]]]  #construct inner_list of [year,month]
            new_list.extend(sub_list)
    
    return new_list

def generate_current_year(end_year,end):
    list1=[]
    zipped_list=[]
    final_list=[]
    for i in range(1,int(end)+1):
        list1.append(i)                  #construct list upto end month
    for i in list1:
        zipped_list=[[end_year,i]]
        final_list.extend(zipped_list)       # zipping till end of month for current year
    return final_list
        

    
    
def for_same_year(month_list,year):
    fresh_list=[]
    zipped_list=[]
    for i in month_list:
        zipped_list=[year,i]
        fresh_list.append(zipped_list)
    return fresh_list
        
    


     

def create_list(start,end,start_year,end_year):     #this function receives 4 argumenst ( start month, end month,start year and end year)
    #and gives a list of all values from start_month to end _month

    new_list=[]
    if(int(start_year) == int(end_year)):
        # this block is if the end date and start date is in the same year
        for i in range(int(start),int(end)+1,1):
            new_list.append(i)
        return for_same_year(new_list,end_year)
        
    else:                              #for diffrent years

        year_list=generate_year(start_year,end_year)   # generating year lists    [2017,2018]
        final_except_last=generate_generic(year_list)     #contains all years except last
    
        
            
        last_year_list=generate_current_year(end_year,end)
        
        
            
    return (final_except_last+last_year_list)





spark_conf=SparkConf()
spark_conf.set("spark.app.name","stack")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()
spark.udf.register("generate_list",create_list,ArrayType(ArrayType(StringType()))) # register UDF

local_list=[["NARESH","HDFC","2017-01-01","2019-03-31"],["ANOOP","ICICI","2017-05-01","2017-07-30"]]


schema=["NAME","COMPANY","START_DATE","END_DATE"]
df=spark.createDataFrame(local_list,schema)
print("printing....")
df.printSchema()

""" |-- NAME: string (nullable = true)
    |-- COMPANY: string (nullable = true)
    |-- START_DATE: string (nullable = true)
    |-- END_DATE: string (nullable = true)
"""

df.show()
"""
+------+-------+----------+----------+
|  NAME|COMPANY|START_DATE|  END_DATE|
+------+-------+----------+----------+
|NARESH|   HDFC|2017-01-01|2017-03-31|
| ANOOP|  ICICI|2017-05-01|2017-07-30|
+------+-------+----------+----------+

"""

month_df=df.select(col("NAME"),col("COMPANY"),\
                   expr("date_format(to_date(START_DATE,'yyyy-MM-dd'),'M') as StartMonth") ,expr("date_format(to_date(END_DATE,'yyyy-MM-dd'),'M') as EndMonth"),\
                   expr("date_format(to_date(START_DATE,'yyyy-MM-dd'),'yyyy') as START_YEAR"),expr("date_format(to_date(END_DATE,'yyyy-MM-dd'),'yyyy') as END_YEAR"))




generated_df=month_df.select(col("NAME"),col("COMPANY"),col("START_YEAR"),col("END_YEAR"),expr("generate_list(StartMonth,EndMonth,START_YEAR,END_YEAR) as listdata"))
#generate list of list by passing to UDF


finaldf=generated_df.select(col("NAME"),col("COMPANY"),expr("explode(listdata) as months_worked")) # get multiple rows

fi=finaldf.select(col("NAME"),col("COMPANY"),expr("concat_ws(',',months_worked) as comma_seperated"))  # convert from list to comma separated

op=fi.select(col("NAME"),col("COMPANY"),expr("substr(comma_seperated,1,4) as YEAR"),expr("lpad(substr(comma_seperated,6),2,0) as MONTH")).show(100)








