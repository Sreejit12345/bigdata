from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import timedelta


     

def(start_date,end_date):
    


spark_conf=SparkConf()
spark_conf.set("spark.app.name","stack")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

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
|NARESH|   HDFC|2017-01-01|2019-03-31|
| ANOOP|  ICICI|2017-05-01|2017-07-30|
+------+-------+----------+----------+

"""

month_df=df.select(col("NAME"),col("COMPANY"),\
                   expr("date_format(to_date(START_DATE,'yyyy-MM-dd'),'M') as StartMonth") ,expr("date_format(to_date(END_DATE,'yyyy-MM-dd'),'M') as EndMonth"),\
                   expr("date_format(to_date(START_DATE,'yyyy-MM-dd'),'yyyy') as START_YEAR"),expr("date_format(to_date(END_DATE,'yyyy-MM-dd'),'yyyy') as END_YEAR"),\
                   expr("date_format(to_date(START_DATE,'yyyy-MM-dd'),'yyyy-MM') as year_month_combo_start"),\
                   expr("date_format(to_date(END_DATE,'yyyy-MM-dd'),'yyyy-MM') as year_month_combo_end"))


spark.udf.register("generate_list",create_list,ArrayType(IntegerType()))  # register UDF

generated_df=month_df.select(col("NAME"),col("COMPANY"),col("year_month_combo"),expr("generate_list(StartMonth,EndMonth,START_YEAR,END_YEAR) as listdata"))# generate list by passing to UDF


finaldf=generated_df.select(col("NAME"),col("COMPANY"),col("year_month_combo"),expr("explode(listdata) as months_worked")) # get multiple rows

finaldf.select(col("NAME"),col("COMPANY"),col("year_month_combo"),expr("lpad(months_worked,2,0) as months_worked")).show(100)  #LPAD to add 0 before month for single digits









