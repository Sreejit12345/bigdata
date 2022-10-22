from pyspark.sql import SparkSession
from pyspark.sql.functions import *



  
spark=SparkSession.builder.master("local[*]").appName("dss").getOrCreate()
 
#spark.sparkContext.addJar("file:///E:/ojdbc6.jar")
 
ip=spark.read.format("jdbc").option("url","jdbc:oracle:thin:@localhost:1521:xe")\
.option("dbtable","entries").option("user","system")\
.option("password","s2d3s2d3")\
.option("driver","oracle.jdbc.driver.OracleDriver").load()

ip.show()
ip.createOrReplaceTempView("temp")

sql_stmt="""
 
select q1.name,q1.total_visits,q2.most_visited_floor,q3.resources_used from
(
(select name,count(*) as total_visits from temp group by name)q1   -- to get total_vists
inner join

(
select name,floor as most_visited_floor from
(
select name,floor,cn,max(cn) over(partition by name order by cn) as m from(    ---- to get most visited
select name,floor,count(*) as cn from temp group by name,floor))
where m=2) q2
on q1.name=q2.name


inner join

(select name,concat_ws(',',collect_set(resources))   as resources_used from temp group by name) q3
 on
(q2.name=q3.name)
)
"""

spark.sql(f"sql_stmt").show()













 
  
  
  
  


