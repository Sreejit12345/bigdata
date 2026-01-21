from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import SparkFiles
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("WebLogETL").getOrCreate()

users_url = "https://raw.githubusercontent.com/ankitbansal6/namastesql_etl_datasets/refs/heads/main/190/users.csv"
logs_url = "https://raw.githubusercontent.com/ankitbansal6/namastesql_etl_datasets/refs/heads/main/190/web_logs.csv"

# use sparkfiles to download and add files to server

spark.sparkContext.addFile(users_url)
spark.sparkContext.addFile(logs_url)

users_path=SparkFiles.get('users.csv')
logs_path=SparkFiles.get('web_logs.csv')

# extract data

users_df=spark.read.format('csv').option('inferSchema',True).option('header',True).option('path',users_path).load()
logs_df=spark.read.format('csv').option('inferSchema',True).option('header',True).option('path',logs_path).load()

#clean data

logs_df=logs_df.filter('int(response_code) in (200,301,302,404)').\
drop_duplicates(['ip_address','url','event_timestamp']).\
withColumn('event_timestamp',F.to_timestamp(F.col('event_timestamp'),'yyyy-MM-dd HH:mm:ss'))

#transform data
logs_df=logs_df.alias('l').join(users_df.alias('ud'),F.col('l.ip_address')==F.col('ud.ip_address'),"left").select('l.*',"ud.user_id").\
withColumn('type',F.lit(1))

logs_df.show(20)

logs_df1=logs_df.withColumn('type',F.lit(2))

logs_df=logs_df.union(logs_df1)

# if we have conseciutive 1's it will break logic since it depends on 1 and then atleast one 0. To handle this, 
# we add in duplicate events with same time ( this will make a 0 come up after a 1 for sure).  For all other metrics it is not going to impact except total events which we should divide by 2

#We make sure the duplicate comes after the original 
# it is not going to matter


# sessionization logic
wn=Window.partitionBy('user_id').orderBy('event_timestamp','type')

logs_df=logs_df.withColumn('rn',F.row_number().over(wn)).\
withColumn('lag_timestamp',F.coalesce(F.lag('event_timestamp').over(wn),F.col('event_timestamp')))

logs_df=logs_df.withColumn('diff',(F.unix_timestamp('event_timestamp')-F.unix_timestamp('lag_timestamp'))/60)


w2=Window.partitionBy('user_id','is_more').orderBy('event_timestamp','type')

logs_df=logs_df.withColumn('is_more',F.when(F.col('diff')>=30,F.lit(1)).otherwise(F.lit(0)))


logs_df=logs_df.withColumn('more_rn',F.row_number().over(w2)).\
withColumn('club_diff',F.col('rn')-F.col('more_rn'))


logs_df=logs_df.withColumn('lead_club_diff',F.lead('club_diff').over(wn))

logs_df=logs_df.withColumn('club_diff',F.when(F.col('is_more')==F.lit(1),F.col('lead_club_diff')).\
						   otherwise(F.col('club_diff'))).drop('lead_club_diff')

w3=Window.partitionBy('user_id').orderBy('club_diff')

logs_df_sessions=logs_df.withColumn('session_id',F.dense_rank().over(w3))



f=logs_df_sessions.groupBy('user_id','session_id').\
agg(F.min('event_timestamp').alias('sessopn_start_time'),
   F.max('event_timestamp').alias('session_end_time'),
   (F.unix_timestamp(F.max('event_timestamp'))-F.unix_timestamp(F.min('event_timestamp'))).alias('session_duration'),
	((F.count('type')/2).cast('int')).alias('total_events'),F.countDistinct(F.col('url')).alias('unique_pages_visit'))



# derive first device used in session

w4=Window.partitionBy('user_id','session_id').orderBy('event_timestamp')

logs_df_sessions=logs_df_sessions.withColumn('rn_session',
											 F.row_number().over(w4))

final=f.alias('f').join(logs_df_sessions.alias('l'), (F.col('f.user_id')==F.col('l.user_id')) &
		(F.col('f.session_id')==F.col('l.session_id'))	&	 
		(F.col('l.rn_session')==1)).\
select('f.*','l.device_type')

final.orderBy('user_id').show(10000)





