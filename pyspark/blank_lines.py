from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import *



spark_conf=SparkConf()

spark_conf.set("spark.master","local[*]")
spark_conf.set("spark.app.name","s")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()



data=[[1,"ABC","XYZ","PUNE","15/07/2022 11:00:00.000"],[1,"ABC","XYZ","MUMBAI","15/07/2022 12:00:00.000"],[1,"ABC","XYZ","Chennai","15/08/2022 14:00:00.000"] \
      ,[1,"ABC","XYZ","Delhi","15/09/2022 17:00:00.000"],[2,"DEF","VGF","DELHI","15/09/2022 17:00:00.000"],[2,"DEF","VGF","WB","15/07/2022 17:00:00.000"]]

schema=["EMP_ID","Name","DEPT","ADDRESS","TIMESTAMP"]

df=spark.createDataFrame(data,schema)


# get month from timestamp and also get current month
temp_df=df.select(col("EMP_ID"),col("NAME"),col("DEPT"),col("ADDRESS"),expr("to_date(TIMESTAMP,'dd/MM/yyyy HH:mm:ss.SSSS') date")\
                                                                                                    ,expr("current_date() current_date"))

df1=temp_df.select(col("EMP_ID"),col("NAME"),col("DEPT"),expr("months_between(current_date,date) diff")).where("diff <=3").drop("diff") # only last 3 months are considered



d=df1.groupBy("EMP_ID").agg(count("*").alias("no_of_times_changed_in_last_3_months")).filter("no_of_times_changed_in_last_3_months > 3").drop("no_of_times_changed_in_last_3_months")
# only consider those employees that have changed more than 3 times


join_cond=d.EMP_ID==temp_df.EMP_ID

ff=d.join(temp_df,join_cond,"inner").drop("d.EMP_ID").dropDuplicates(["NAME","DEPT"])  # join with original df, dropping to handle case if same emp works in multiple depts

ff.groupBy("DEPT").count().show()    # show department wise count


