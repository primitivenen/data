import subprocess
import sys
import findspark
findspark.init('/usr/hdp/current/spark2-client')

import pyspark
from pyspark.sql.functions import lit, col, instr, expr, pow, round, bround, corr, count, mean, stddev_pop, min, max
from pyspark.sql.functions import monotonically_increasing_id, initcap, lower, upper, ltrim, rtrim, rpad, lpad, trim
from pyspark.sql.functions import regexp_replace, translate, regexp_extract, current_date, current_timestamp, struct
from pyspark.sql.functions import date_add, date_sub, datediff, months_between, to_date, to_timestamp, coalesce, split, size
from pyspark.sql.functions import array_contains, explode, udf
from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, when

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType, FloatType, LongType

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import desc
import pandas as pd
from datetime import date, timedelta, datetime

def get_Spark():

    conf = pyspark.SparkConf().setAll([
        ('spark.submit.deployMode', 'client'), # deploy in yarn-client or yarn-cluster
        ('spark.executor.memory', '8g'),       # memory allocated for each executor
        ('spark.executor.cores', '3'),         # number of cores for each executor
        ('spark.executor.instances', '20'),    # number of executors in total
        ('spark.yarn.am.memory', '20g')])      # memory for spark driver (application master)
    spark = SparkSession.builder \
    .master("yarn") \
    .appName("name") \
    .enableHiveSupport() \
    .config(conf = conf) \
    .getOrCreate()

    return spark

spark = get_Spark()
spark_context = spark.sparkContext
hc = HiveContext(spark_context)

normaltimeFormat = "yyyyMMddHHmmss"

def run_cmd(args_list):
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proc.communicate()
    return proc.returncode
    
def build_schema(model):
    types = {
        "string": StringType(),
        "long": LongType(),
        "date": StringType(),
        "categorical - integer": IntegerType(),
        "double": DoubleType(),
        "integer": DoubleType(),
        "int": IntegerType(),
        "float": FloatType()
    }

    with open('schemas/{}_schema.csv'.format(model if model != "conv" else model + "_1s"), 'r') as lines:
        columns = []
        for line in lines:
            lineArray = line.split(",")
            columns.append(StructField(lineArray[1], types[lineArray[2]]))
        schema = StructType(columns)
    return schema
    
def load1sDataPerDay(day, dfSchema):
    data_file = "hdfs://namenode:8020/apps/hive/warehouse/conv_tsp_tbls.db/a7m_1s_orc/dt=" + str(day)
    returncode = run_cmd(['hdfs', 'dfs', '-test', '-e', data_file])
    if returncode:
        print('{} does not exist, skipping ..'.format(data_file))
    else :
        return spark.read.format("orc").schema(dfSchema).load(data_file)#.where("vin='LMGMS1G85J1024832'")
        
def load5sDataPerDay(day):
    data_file = "hdfs://namenode:8020/apps/hive/warehouse/conv_tsp_tbls.db/a7m_5s_orc/dt=" + str(day) + "/000000_0"
    returncode = run_cmd(['hdfs', 'dfs', '-test', '-e', data_file])
    if returncode:
        print('{} does not exist, skipping ..'.format(data_file))
    else :
        return spark.read.format("orc").load(data_file).select("_col0", "_col1", "_col19")\
            .withColumnRenamed("_col0", "vin").withColumnRenamed("_col1", "normaltime")\
            .withColumnRenamed("_col19", "icm_totalodometer")#.where("vin='LMGMS1G85J1024832'")

def generate_trip(df):
	res = {}
	res['vin'] = []
	res['start_time'] = []
	res['end_time'] = []
	res['start_lat'] = []
	res['start_lon'] = []
	res['end_lat'] = []
	res['end_lon'] = []
	res['distance']  = []

	#x = df.select("vin", "prev_diff", "next_diff", "normaltime", "prev_normaltime", "next_normaltime", "icm_totalodometer", "tel_latitudedeg")
	#x.show(20)

	df = df.sort(["vin","normaltime"], ascending=[0,1])
	pdf = df.toPandas()
    
	# get indices where time difference longer than threshold
	indices = pdf.index[(pdf['prev_diff'] >= int(60)) | (pdf['next_diff'] >= int(60))].tolist()

	for i in range(len(indices) - 1):
		lo = indices[i]
		hi = indices[i+1]

		if pdf['vin'].iloc[lo] != pdf['vin'].iloc[hi]:
			#print ('{} finished'.format(pdf['vin'].iloc[lo]))
			continue
			
		if pdf['prev_diff'].iloc[lo] < int(60) or pdf['next_diff'].iloc[hi] < int(60) : 
			continue
		
		if pdf['icm_totalodometer'].iloc[hi] - pdf['icm_totalodometer'].iloc[lo] < 1 and int((pdf['normaltime'].iloc[hi] - pdf['normaltime'].iloc[lo]).total_seconds()) < 600 :
			continue
		
		res['vin'].append(pdf['vin'].iloc[lo])
		res['start_time'].append(pdf['normaltime'].iloc[lo])
		res['start_lat'].append(pdf['tel_latitudedeg'].iloc[lo] + pdf['tel_latitudemin'].iloc[lo] / 60.0 \
								   + pdf['tel_latitudesec'].iloc[lo] / 3600.0)
		res['start_lon'].append(pdf['tel_longitudedeg'].iloc[lo] + pdf['tel_longitudemin'].iloc[lo] / 60.0 \
								   + pdf['tel_longitudesec'].iloc[lo] / 3600.0)
		res['end_time'].append(pdf['normaltime'].iloc[hi])
		res['end_lat'].append(pdf['tel_latitudedeg'].iloc[hi] + pdf['tel_latitudemin'].iloc[hi] / 60.0 \
								   + pdf['tel_latitudesec'].iloc[hi] / 3600.0)
		res['end_lon'].append(pdf['tel_longitudedeg'].iloc[hi] + pdf['tel_longitudemin'].iloc[hi] / 60.0 \
								   + pdf['tel_longitudesec'].iloc[hi] / 3600.0)
		res['distance'].append(pdf['icm_totalodometer'].iloc[hi] - pdf['icm_totalodometer'].iloc[lo])

	res_df = pd.DataFrame(res)

	res_df['duration'] = res_df.apply(lambda x: int((x['end_time']-x['start_time']).total_seconds())/3600.0, axis=1)
	res_df['speed'] = res_df.apply(lambda x: float(x['distance'] / x['duration']), axis=1)

	#res_df.head(100)
	res_df = res_df[res_df.duration<=6]
        return res_df

def extract_data(days):
	my_window = Window.partitionBy("vin").orderBy("normaltime")
	my_next_window = Window.partitionBy("vin").orderBy(desc("normaltime"))

	dfSchema = build_schema("conv")
	df = None
	for day in days:
                day = day.strftime("%Y%m%d")
		df_1s = load1sDataPerDay(day, dfSchema)
		df_5s = load5sDataPerDay(day)

		if not (df_1s is None or df_5s is None):
			df_tmp = df_1s.join(df_5s, ["vin", "normaltime"], "inner").withColumn("normaltime", to_timestamp(col("normaltime"), normaltimeFormat))
			df_tmp = df_tmp.where("tel_latitudedeg > 0 and tel_longitudedeg > 0")
			df_tmp = df_tmp.withColumn("next_normaltime", F.lag(df_tmp.normaltime).over(my_next_window))
			df_tmp = df_tmp.withColumn("prev_normaltime", F.lag(df_tmp.normaltime).over(my_window))
			df_tmp = df_tmp.withColumn("prev_diff", F.when(F.isnull(df_tmp.normaltime.cast("long") - df_tmp.prev_normaltime.cast("long")), 1000).otherwise(df_tmp.normaltime.cast("long") - df_tmp.prev_normaltime.cast("long")))
			df_tmp = df_tmp.withColumn("next_diff", F.when(F.isnull(df_tmp.next_normaltime.cast("long") - df_tmp.normaltime.cast("long")), 1000).otherwise(df_tmp.next_normaltime.cast("long") - df_tmp.normaltime.cast("long")))
			df_tmp = df_tmp.where("prev_diff >= 60 or next_diff >= 60")
			#print('{} starting/ending rows  ..'.format(df_tmp.count()))
			if df is None:
				df = df_tmp
			else:
				df = df.union(df_tmp)
		print('{} processing ..'.format(day))
		if not (df is None):
			print('{} rows loaded ..'.format(df.count()))
	return df	
	
def clean_trip(hc):

	print('Trip Clean Start')

	sql = 'drop table if exists ubi.conv_trips_complete purge' 
        hc.sql("""{}""".format(sql))
	
	sql = """create table ubi.conv_trips_complete as 
		select distinct * from 
		( 
		select vin, start_time, start_lat as start_loc_lat, start_lon as start_loc_lon, end_time, end_lat as end_loc_lat, end_lon as end_loc_lon, distance, duration, speed, from_unixtime(unix_timestamp(CAST(TO_DATE(start_time) as date), 'yyyy-MM-dd'),'yyyyMMdd') as start_day from (select vin, from_unixtime(cast(start_time/1000000000 as bigint)) as start_time, start_lat, start_lon, from_unixtime(cast(end_time/1000000000 as bigint)) as end_time, end_lat, end_lon, distance, duration, speed from (select b.* from (select conv_trips.* from ubi.conv_trips join (select vin, start_time, max(end_time) as end_time from ubi.conv_trips group by vin, start_time) a on conv_trips.vin = a.vin and conv_trips.start_time = a.start_time and conv_trips.end_time = a.end_time) b join (select vin, end_time, min(start_time) as start_time from ubi.conv_trips group by vin, end_time) c on b.vin = c.vin and b.start_time = c.start_time and b.end_time = c.end_time) e ) f ) g where g.distance % 5 = 0"""
        hc.sql(sql)
	
	sql = """insert into table ubi.conv_trips_complete select distinct * from 
        (select vin, start_time, start_lat as start_loc_lat, start_lon as start_loc_lon, end_time, end_lat as end_loc_lat, end_lon as end_loc_lon, distance, duration, speed, from_unixtime(unix_timestamp(CAST(TO_DATE(start_time) as date), 'yyyy-MM-dd'),'yyyyMMdd') as start_day from (select vin, from_unixtime(cast(start_time/1000000000 as bigint)) as start_time, start_lat, start_lon, from_unixtime(cast(end_time/1000000000 as bigint)) as end_time, end_lat, end_lon, distance, duration, speed from (select b.* from (select conv_trips.* from ubi.conv_trips join (select vin, start_time, max(end_time) as end_time from ubi.conv_trips group by vin, start_time) a on conv_trips.vin = a.vin and conv_trips.start_time = a.start_time and conv_trips.end_time = a.end_time) b join (select vin, end_time, min(start_time) as start_time from ubi.conv_trips group by vin, end_time) c on b.vin = c.vin and b.start_time = c.start_time and b.end_time = c.end_time) e ) f ) g where g.distance % 5 = 1"""
        hc.sql(sql)
	
	sql = """insert into table ubi.conv_trips_complete select distinct * from 
        (select vin, start_time, start_lat as start_loc_lat, start_lon as start_loc_lon, end_time, end_lat as end_loc_lat, end_lon as end_loc_lon, distance, duration, speed, from_unixtime(unix_timestamp(CAST(TO_DATE(start_time) as date), 'yyyy-MM-dd'),'yyyyMMdd') as start_day from 
        (select vin, from_unixtime(cast(start_time/1000000000 as bigint)) as start_time, start_lat, start_lon, from_unixtime(cast(end_time/1000000000 as bigint)) as end_time, end_lat, end_lon, distance, duration, speed from (select b.* from (select conv_trips.* from ubi.conv_trips join (select vin, start_time, max(end_time) as end_time from ubi.conv_trips group by vin, start_time) a on conv_trips.vin = a.vin and conv_trips.start_time = a.start_time and conv_trips.end_time = a.end_time) b join (select vin, end_time, min(start_time) as start_time from ubi.conv_trips group by vin, end_time) c on b.vin = c.vin and b.start_time = c.start_time and b.end_time = c.end_time) e ) f ) g where g.distance % 5 = 2"""
        hc.sql(sql)
	
	sql = """insert into table ubi.conv_trips_complete select distinct * from 
        (select vin, start_time, start_lat as start_loc_lat, start_lon as start_loc_lon, end_time, end_lat as end_loc_lat, end_lon as end_loc_lon, distance, duration, speed, from_unixtime(unix_timestamp(CAST(TO_DATE(start_time) as date), 'yyyy-MM-dd'),'yyyyMMdd') as start_day from (select vin, from_unixtime(cast(start_time/1000000000 as bigint)) as start_time, start_lat, start_lon, from_unixtime(cast(end_time/1000000000 as bigint)) as end_time,  
        end_lat, end_lon, distance, duration, speed from (select b.* from (select conv_trips.* from ubi.conv_trips join 
        (select vin, start_time, max(end_time) as end_time from ubi.conv_trips group by vin, start_time) a on conv_trips.vin = a.vin and conv_trips.start_time = a.start_time and conv_trips.end_time = a.end_time) b  join (select vin, end_time, min(start_time) as start_time from ubi.conv_trips group by vin, end_time) c 
        on b.vin = c.vin and b.start_time = c.start_time and b.end_time = c.end_time) e ) f ) g where g.distance % 5 = 3"""
        hc.sql(sql)
	
	sql = """insert into table ubi.conv_trips_complete select distinct * from 
        (select vin, start_time, start_lat as start_loc_lat, start_lon as start_loc_lon, end_time, end_lat as end_loc_lat, end_lon as end_loc_lon, distance, duration, speed, from_unixtime(unix_timestamp(CAST(TO_DATE(start_time) as date), 'yyyy-MM-dd'),'yyyyMMdd') as start_day from (select vin, from_unixtime(cast(start_time/1000000000 as bigint)) as start_time, start_lat, start_lon, from_unixtime(cast(end_time/1000000000 as bigint)) as end_time,  
        end_lat, end_lon, distance, duration, speed from (select b.* from (select conv_trips.* from ubi.conv_trips join 
        (select vin, start_time, max(end_time) as end_time from ubi.conv_trips group by vin, start_time) a on conv_trips.vin = a.vin and conv_trips.start_time = a.start_time and conv_trips.end_time = a.end_time) b join (select vin, end_time, min(start_time) as start_time from ubi.conv_trips group by vin, end_time) c 
        on b.vin = c.vin and b.start_time = c.start_time and b.end_time = c.end_time) e ) f ) g where g.distance % 5 = 4"""
        hc.sql(sql)
	
	print('Trip Clean End')

def metric_generation(hc):

    print('Metric Generation Start')
	
    sql = """drop table if exists ubi.am_peak_driving_by_day"""
    hc.sql(sql)
	
    sql = """create table ubi.am_peak_driving_by_day as
	select vin, start_day, sum(daily_peak_driving_mins) as daily_peak_driving_mins, 
sum(daily_driving_mins) as daily_driving_mins, sum(daily_peak_driving_mins)/sum(daily_driving_mins) as percent_daily_peak_driving 
from (select e.vin, e.start_day, d.daily_peak_driving_mins, d.daily_driving_mins
from
(select vin, start_day, min(start_time) as start_time from ubi.conv_trips_complete group by vin, start_day) e
inner join
(select vin, start_day, from_unixtime(unix_timestamp(date_add(from_unixtime(unix_timestamp(start_day,'yyyyMMdd'),'yyyy-MM-dd'), 30),'yyyy-MM-dd'),'yyyyMMdd') as thirty_day_after, 
sum(peak_driving_mins) as daily_peak_driving_mins, 
sum(driving_mins) as daily_driving_mins from 
(select vin, start_day, (unix_timestamp(b.end_time)-unix_timestamp(b.start_time))/60 as driving_mins,  
 (case when b.peak_end_time > b.peak_start_time then (unix_timestamp(b.peak_end_time)-unix_timestamp(b.peak_start_time))/60 else 0 end) as peak_driving_mins from 
 (
 select vin, start_day, start_time, a.peak_start, case
  when start_time > (a.peak_start) then cast(start_time as timestamp)
  else cast(a.peak_start as timestamp)
  end as peak_start_time,
  end_time, a.peak_end, case
  when end_time < (a.peak_end) then cast(end_time as timestamp)
  else cast(a.peak_end as timestamp)
  end as peak_end_time
  from 
  (select vin, start_time, start_day, end_time, 
		from_unixtime(unix_timestamp(CONCAT(start_day, ' ', '07:00:00'), 'yyyyMMdd HH:mm:ss')) as peak_start,
	   from_unixtime(unix_timestamp(CONCAT(start_day, ' ', '09:00:00'), 'yyyyMMdd HH:mm:ss')) as peak_end 
		from ubi.conv_trips_complete) a
		) b
		) c
group by vin, start_day ) d
on e.vin = d.vin
where e.start_day <= d.thirty_day_after and e.start_day > d.start_day
) f
group by vin, start_day
order by start_day desc, daily_peak_driving_mins desc"""
    hc.sql(sql)
	
    print("============================")
    print("======AM_PEAK_DRIVE=========")
    print("=========created============")
    print("============================")
	
    sql = """drop table if exists ubi.long_distance_by_day"""
    hc.sql(sql)
	
    sql = """create table ubi.long_distance_by_day as
select vin, start_day, sum(frequency) as frequency, avg(percent_long_distance) as percent_long_distance
from
(
select c.vin, c.start_day, b.frequency, b.percent_long_distance
from
(select vin, start_day, min(start_time) as start_time from ubi.conv_trips_complete group by vin, start_day
) c
inner join
(
select vin, sum(long_distance) as frequency, avg(long_distance) as percent_long_distance, start_day,
from_unixtime(unix_timestamp(date_add(from_unixtime(unix_timestamp(start_day,'yyyyMMdd'),'yyyy-MM-dd'), 30),'yyyy-MM-dd'),'yyyyMMdd') as thirty_day_after
from 
(select vin, distance, start_day, case
  when distance >= 100 then 1
  else 0
  end as long_distance
 from ubi.conv_trips_complete) a 
group by vin, start_day) b
on c.vin = b.vin
where c.start_day <= b.thirty_day_after and c.start_day > b.start_day
) d
group by vin, start_day
order by frequency desc"""
    hc.sql(sql)
	
    print("============================")
    print("======LONG_DISTANCE=========")
    print("=========created============")
    print("============================")

    sql = """drop table if exists ubi.night_driving_by_day"""
    hc.sql(sql)
    
    sql = """create table ubi.night_driving_by_day as
select vin, start_day, sum(daily_night_driving_mins) as daily_night_driving_mins, 
sum(daily_driving_mins) as daily_driving_mins, sum(daily_night_driving_mins)/sum(daily_driving_mins) as percent_daily_night_driving from 

(
select e.vin, e.start_day, d.daily_driving_mins, d.daily_night_driving_mins

from

(select vin, start_day, min(start_time) as start_time from ubi.conv_trips_complete group by vin, start_day
) e

inner join

(select vin, start_day,from_unixtime(unix_timestamp(date_add(from_unixtime(unix_timestamp(start_day,'yyyyMMdd'),'yyyy-MM-dd'), 30),'yyyy-MM-dd'),'yyyyMMdd') as thirty_day_after,
sum(night_driving_mins) as daily_night_driving_mins,sum(driving_mins) as daily_driving_mins from

(
select vin, start_day, 
(unix_timestamp(b.end_time)-unix_timestamp(b.start_time))/60 as driving_mins,  
(case when b.night_end_time > b.night_start_time then (unix_timestamp(b.night_end_time)-unix_timestamp(b.night_start_time))/60 else 0 end) as night_driving_mins 
from 

(select vin, start_day, start_time,a.night_start, case
  when start_time > (a.night_start) then cast(start_time as timestamp)
  else cast(a.night_start as timestamp)
  end as night_start_time,
  end_time, a.night_end, case
  when end_time < (a.night_end) then cast(end_time as timestamp)
  else cast(a.night_end as timestamp)
  end as night_end_time
  
  from 
  
  (select vin, start_time, start_day, end_time, 
		from_unixtime(unix_timestamp(CONCAT(start_day, ' ', '22:00:00'), 'yyyyMMdd HH:mm:ss')) as night_start,
	   from_unixtime(unix_timestamp(CONCAT(start_day, ' ', '05:00:00'), 'yyyyMMdd HH:mm:ss') + 86400) as night_end from ubi.conv_trips_complete
    ) a
		
		) b
		
		) c
		
group by vin, start_day) d

on e.vin = d.vin
where e.start_day <= d.thirty_day_after and e.start_day > d.start_day

)f
group by vin, start_day
order by start_day desc, daily_night_driving_mins desc"""
    hc.sql(sql)
	
    print("============================")
    print("======NIGHT_DRIVE===========")
    print("=========created============")
    print("============================")

    sql = """drop table if exists ubi.pm_peak_driving_by_day"""
    hc.sql(sql)
	
    sql = """create table ubi.pm_peak_driving_by_day as
	select vin, start_day, sum(daily_peak_driving_mins) as daily_peak_driving_mins, 
sum(daily_driving_mins) as daily_driving_mins, sum(daily_peak_driving_mins)/sum(daily_driving_mins) as percent_daily_peak_driving 
from
(select e.vin, e.start_day, d.daily_peak_driving_mins, d.daily_driving_mins
from
(select vin, start_day, min(start_time) as start_time from ubi.conv_trips_complete group by vin, start_day
) e
inner join
(select vin, start_day, from_unixtime(unix_timestamp(date_add(from_unixtime(unix_timestamp(start_day,'yyyyMMdd'),'yyyy-MM-dd'), 30),'yyyy-MM-dd'),'yyyyMMdd') as thirty_day_after, sum(peak_driving_mins) as daily_peak_driving_mins, 
sum(driving_mins) as daily_driving_mins from 
(select vin, start_day, (unix_timestamp(b.end_time)-unix_timestamp(b.start_time))/60 as driving_mins,  
 (case when b.peak_end_time > b.peak_start_time then (unix_timestamp(b.peak_end_time)-unix_timestamp(b.peak_start_time))/60 else 0 end) as peak_driving_mins from 
 (
 select vin, start_day, start_time, a.peak_start, case
  when start_time > (a.peak_start) then cast(start_time as timestamp)
  else cast(a.peak_start as timestamp)
  end as peak_start_time,
  end_time, a.peak_end, case
  when end_time < (a.peak_end) then cast(end_time as timestamp)
  else cast(a.peak_end as timestamp)
  end as peak_end_time
  from 
  (select vin, start_time, start_day, end_time, 
		from_unixtime(unix_timestamp(CONCAT(start_day, ' ', '16:00:00'), 'yyyyMMdd HH:mm:ss')) as peak_start,
	   from_unixtime(unix_timestamp(CONCAT(start_day, ' ', '19:00:00'), 'yyyyMMdd HH:mm:ss')) as peak_end 
		from ubi.conv_trips_complete) a
		) b
		) c		
group by vin, start_day ) d
on e.vin = d.vin
where e.start_day <= d.thirty_day_after and e.start_day > d.start_day
) f
group by vin, start_day
order by start_day desc, daily_peak_driving_mins desc
	"""
    hc.sql(sql)
	
    print("============================")
    print("======PM_PEAK_DRIVE=========")
    print("=========created============")
    print("============================")
    
    sql = """drop table if exists ubi.entropy"""
    hc.sql(sql)
	
    sql = """create table ubi.entropy as
select e.vin, d.start_day, e.entropy_all, d.entropy_last_month from
(
	select vin, sum(- probability * log2(probability)) as entropy_all 
	from 
	(
		select vin, start_loc_lat, 
			  start_loc_lon, 
			  end_loc_lat, 
			  end_loc_lon, 
			  sum( probability ) as probability 
		from
		(
			select conv_trips_complete.vin, ROUND(CAST(start_loc_lat as float), 3) as start_loc_lat, 
				  ROUND(CAST(start_loc_lon as float), 3) as start_loc_lon, 
				  ROUND(CAST(end_loc_lat as float), 3) as end_loc_lat, 
				  ROUND(CAST(end_loc_lon as float), 3) as end_loc_lon, 
				  1.0 / f.totalfrequency as probability 
			 from ubi.conv_trips_complete join (select vin, count(*) as totalfrequency from ubi.conv_trips_complete 
														group by vin
			 ) f 
			 on conv_trips_complete.vin = f.vin 
		) g
		 group by vin, start_loc_lat, start_loc_lon, end_loc_lat, end_loc_lon 
	)h
	group by vin 

) e 
join
(
	select vin, start_day, sum(- probability * log2(probability)) as entropy_last_month 
	from 
	(
		select vin, start_day, start_loc_lat, 
			  start_loc_lon, 
			  end_loc_lat, 
			  end_loc_lon, 
			  sum( probability ) as probability 
		from
		(
			select conv_trips_complete.vin, start_day, ROUND(CAST(start_loc_lat as float), 3) as start_loc_lat, 
				  ROUND(CAST(start_loc_lon as float), 3) as start_loc_lon, 
				  ROUND(CAST(end_loc_lat as float), 3) as end_loc_lat, 
				  ROUND(CAST(end_loc_lon as float), 3) as end_loc_lon, 
				  1.0 / a.totalfrequency as probability 
			 from ubi.conv_trips_complete 
		  join 
		  (select vin, count(*) as totalfrequency from (select * from (
select vin, start_day, 
rank() over ( partition by vin order by start_day desc) as rank 
from ubi.conv_trips_complete) t where rank <= 30) a
												group by vin
			 ) a 
			 on conv_trips_complete.vin = a.vin
		) c
		 group by vin, start_day, start_loc_lat, start_loc_lon, end_loc_lat, end_loc_lon 
	)b
	group by vin,start_day
) d
on e.vin=d.vin """ 
    hc.sql(sql)
	
    print("============================")
    print("=========Entropy============")
    print("=========created============")
    print("============================")
	
    sql = """drop table if exists ubi.conv_trip_metrics"""
    hc.sql(sql)
	
    sql = """create table ubi.conv_trip_metrics as
select h.*, g.entropy_all, g.entropy_last_month 
from ubi.entropy g join
(
select e.vin, e.start_day, e.daily_night_driving_mins, e.percent_daily_night_driving,
long_distance_count, percent_long_distance, percent_am_peak, am_peak_mins, percent_pm_peak, pm_peak_mins
from ubi.night_driving_by_day e join
(
select c.vin, c.start_day, c.frequency as long_distance_count, c.percent_long_distance, percent_am_peak, 
am_peak_mins, percent_pm_peak, pm_peak_mins
from ubi.long_distance_by_day c join
(
select a.vin, a.start_day, a.percent_daily_peak_driving as percent_am_peak, a.daily_peak_driving_mins as am_peak_mins, 
b.percent_daily_peak_driving as percent_pm_peak, b.daily_peak_driving_mins as pm_peak_mins
from ubi.am_peak_driving_by_day a join ubi.pm_peak_driving_by_day b
on a.vin=b.vin and a.start_day=b.start_day) d
on c.vin=d.vin and c.start_day=d.start_day) f
on e.vin=f.vin and e.start_day=f.start_day) h
on h.vin = g.vin and h.start_day=g.start_day"""
    hc.sql(sql)
    
    print("============================")
    print("=====conv_trip_metrics======")
    print("=========created============")
    print("============================")
	
    sql = """drop table if exists ubi.trip_metrics"""
    hc.sql(sql)
	
    sql = """create table ubi.trip_metrics as 
select *, 'conv' as vintype, 'conv' as subtype from ubi.conv_trip_metrics
union all
select guobiao_trip_metrics.*, 'guobiao' as vintype, vintypes.vintype as subtype  
 from ubi.guobiao_trip_metrics join ubi.vintypes 
 on guobiao_trip_metrics.vin=vintypes.vin"""
    hc.sql(sql)
	
    print('Trip Metric Generation End')
	
def main():

    args = sys.argv[1:]
    start_date = datetime.strptime(args[0],'%Y%m%d')
    end_date = datetime.strptime(args[1],'%Y%m%d')
    print("The start date is "+args[0])
    print("The end date is "+args[1])
	
    days = [start_date + timedelta(days = x) for x in range(0, (end_date-start_date).days)]
    del args[0:1]

    df = extract_data(days)
    if df is None:
	print("extract_data is done. ")
	return
    res_df = generate_trip(df)


    # export to hive
    my_col = ["vin"
				   ,"start_time"
				   ,"start_lat"
				   ,"start_lon"
				   ,"end_time"
				   ,"end_lat"
				   ,"end_lon"
				   ,"distance"
				   ,"duration"
				   ,"speed"]

    mySchema = StructType([StructField("vin", StringType(), True)\
				   ,StructField("start_time", StringType(), True)\
				   ,StructField("start_lat", DoubleType(), True)\
				   ,StructField("start_lon", DoubleType(), True)\
				   ,StructField("end_time", StringType(), True)\
				   ,StructField("end_lat", DoubleType(), True)\
				   ,StructField("end_lon", DoubleType(), True)\
				   ,StructField("distance", DoubleType(), True)\
				   ,StructField("duration", DoubleType(), True)\
				   ,StructField("speed", DoubleType(), True)])
    spark_df = hc.createDataFrame(res_df[my_col], mySchema)
    cols = [when(~col(x).isin("NULL", "NA", "NaN",""), col(x)).alias(x) for x in spark_df.columns]
    spark_df = spark_df.select(*cols)
    spark_df.registerTempTable('update_dataframe')

    sql_cmd = """INSERT INTO TABLE ubi.conv_trips SELECT * from update_dataframe"""
    print(sql_cmd)
    hc.sql(sql_cmd)
    print('Table address creation done.')
	
    clean_trip(hc)
    
    metric_generation(hc)
   
    
if __name__ == '__main__':
    main()    
