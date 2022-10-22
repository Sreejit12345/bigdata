def generate_hive_commands_orders_data():
	command_1="hive -e \"create external table if not exists orders_ext(order_id int,order_date string,cust_id int,status string) row format delimited fields terminated by ',' location '/user/root/spark_output_airflow'\""
	command_2="""hive -e \"create table if not exists hive_hbase_table(customer_id int, customer_fname string, customer_lname string,customer_email string,customer_password string,customer_street string,customer_city string,customer_state string,customer_zipcode string,order_id int,order_date string,order_status string) stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES(\\"hbase.columns.mapping\\"=\\":key,cf1:customer_fname,cf1:customer_lname,cf1:customer_email,cf1:customer_password,cf1:customer_street,cf1:customer_city,cf1:customer_state,cf1:customer_zipcode,cf1:order_id,cf1:order_date,cf1:order_status\\")\""""""
	command_3="""hive -e \"insert into hive_hbase_table select c.customer_id,c.customer_fname,c.customer_lname,c.customer_email,c.customer_password,c.customer_street,c.customer_city,c.customer_state,c.customer_zipcode,o.order_id,o.order_date,o.status from cust c inner join orders_ext o on(c.customer_id=o.cust_id)\""""

	return f'{command_1}&&{command_2}&&{command_3}'


print(generate_hive_commands_orders_data())
