from rev import *
from pyspark.sql.types import StructType,StructField,IntegerType,TimestampType,StringType
from pyspark import StorageLevel

def agecheck(age):
    if(age>18):
        return 'Y'
    else:
        return 'N'


def createNew():

    spark= createSession()
    df1=spark.read.format("csv").option("inferSchema",True).option("mode","FAILFAST").option("inferSchema",True).option("header",True).option("path","E:\\spark practice\\-201125-161348.dataset1").load()
    df2=df1.toDF("NAME","AGE","CITY")

    #spark.udf.register("abc",agecheck,StringType())

    abc=udf(agecheck,StringType())
    #df2.select(col("NAME"),col("AGE"),col("CITY"),expr("case when AGE>18 then 'Y' else 'N' end check")).show()
    df3=df2.withColumn("isadult",abc(col("AGE"))).persist(StorageLevel.DISK_ONLY_2)
    print(df3.rdd.getNumPartitions())
    print(spark.sparkContext.defaultMinPartitions)
    
    
    





createNew()

