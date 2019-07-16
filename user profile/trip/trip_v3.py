import sys
import geocoder
import pandas as pd
import os
import numpy as np
from datetime import datetime, date, timedelta


import logging

sys.path.insert(0, "/home/prod/git/common/logging")

from user_logging import assign_log_handler, close_log_handler



# Build spark session
import findspark

# spark location on namenode server
findspark.init("/usr/hdp/current/spark2-client")
import pyspark
conf = pyspark.SparkConf().setAll([('spark.app.name', 'guobiao_tsp_tbls.trip_map'), # App Name
    ('spark.yarn.queue', 'prod'),
    ('spark.master', 'yarn'),              # spark run mode: locally or remotely
    ('spark.submit.deployMode', 'client'), # deploy in yarn-client or yarn-cluster
    ('spark.executor.memory', '18g'),       # memory allocated for each executor, 10g
    ('spark.yarn.executor.memoryOverhead','18g'),
    #('spark.memory.fraction', '0.7'),
    ('spark.executor.cores', '6'),         # number of cores for each executor,3
    ('spark.executor.instances', '20'),    # number of executors in total
    ('spark.driver.maxResultSize', '10g'), # Result size is large, need to increase from default of 1g,5g
    ('spark.yarn.am.memory', '20g')])       # memory for spark driver (application master),10g
sc = pyspark.SparkContext.getOrCreate(conf=conf)

from pyspark.sql import HiveContext
from pyspark.sql.functions import col, when, isnan,isnull

# Hive context
hc = HiveContext(sc)

def GenerateTrips(sc):
    from pyspark.sql import HiveContext
    
    # Hive context
    hc = HiveContext(sc) 
        
    sql = 'DROP TABLE IF EXISTS guobiao_tsp_tbls.starts_latest PURGE' 
    hc.sql("""{}""".format(sql))

    sql = 'create table guobiao_tsp_tbls.starts_latest as select vin, veh_odo, max(ts_seconds) as ts_seconds from \
        guobiao_tsp_tbls.starts_redundant group by vin, veh_odo'
    hc.sql("""{}""".format(sql))
    
    print("============================")
    print("=======starts_redundant=====")
    print("=========created============")
    print("============================")

    sql = 'DROP TABLE IF EXISTS guobiao_tsp_tbls.starts PURGE' 
    hc.sql("""{}""".format(sql))

    sql = 'create table guobiao_tsp_tbls.starts as select starts_redundant.* from \
        guobiao_tsp_tbls.starts_redundant inner join guobiao_tsp_tbls.starts_latest \
        on starts_redundant.vin = starts_latest.vin and starts_redundant.veh_odo == starts_latest.veh_odo and starts_redundant.ts_seconds = starts_latest.ts_seconds'
    hc.sql("""{}""".format(sql))

    print("============================")
    print("=======starts===============")
    print("=========created============")
    print("============================")
    
    sql = 'DROP TABLE IF EXISTS guobiao_tsp_tbls.ends_latest PURGE' 
    hc.sql("""{}""".format(sql))
    
    sql = 'create table guobiao_tsp_tbls.ends_latest as select vin, veh_odo, min(ts_seconds) as ts_seconds \
    from guobiao_tsp_tbls.ends_redundant group by vin, veh_odo'
    hc.sql("""{}""".format(sql))
    
    print("============================")
    print("=======ends_latest==========")
    print("=========created============")
    print("============================")
    
    sql = 'DROP TABLE IF EXISTS guobiao_tsp_tbls.ends PURGE' 
    hc.sql("""{}""".format(sql))

    sql = 'create table guobiao_tsp_tbls.ends as select ends_redundant.* \
        from guobiao_tsp_tbls.ends_redundant inner join guobiao_tsp_tbls.ends_latest \
        on ends_redundant.vin = ends_latest.vin and ends_redundant.veh_odo == ends_latest.veh_odo \
        and ends_redundant.ts_seconds = ends_latest.ts_seconds'
    hc.sql("""{}""".format(sql))
    
    print("============================")
    print("=======ends=================")
    print("=========created============")
    print("============================")
    
    sql = 'drop table if exists guobiao_tsp_tbls.trip_candidates purge' 
    hc.sql("""{}""".format(sql))
    
    sql = 'create table guobiao_tsp_tbls.trip_candidates as select starts.vin, starts.loc_lat as start_loc_lat, starts.loc_lon as start_loc_lon, \
    from_utc_timestamp(to_utc_timestamp(from_unixtime(starts.ts_seconds), "America/Los_Angeles"), "Asia/Shanghai") as start_time, \
    starts.day as start_day, ends.loc_lat as end_loc_lat, ends.loc_lon as end_loc_lon, from_utc_timestamp(to_utc_timestamp(from_unixtime(ends.ts_seconds), "America/Los_Angeles"), "Asia/Shanghai") as end_time, \
    ends.veh_odo - starts.veh_odo as distance \
    from guobiao_tsp_tbls.starts inner join guobiao_tsp_tbls.ends on starts.vin = ends.vin \
    where ends.veh_odo >= starts.veh_odo and ends.ts_seconds > starts.ts_seconds and ends.ts_seconds - starts.ts_seconds < 50000 and ends.veh_odo - starts.veh_odo < 1000'
    hc.sql("""{}""".format(sql))
    
    print("============================")
    print("=======trip_candidates======")
    print("=========created============")
    print("============================")

    sql = 'drop table if exists guobiao_tsp_tbls.trip_distance purge' 
    hc.sql("""{}""".format(sql))
    
    sql = 'create table guobiao_tsp_tbls.trip_distance as select vin, start_time, min(distance) as distance \
    from guobiao_tsp_tbls.trip_candidates group by vin, start_time'
    hc.sql("""{}""".format(sql))

    print("============================")
    print("=======trip_distance========")
    print("=========created============")
    print("============================")
    
    sql = 'drop table if exists ubi.trips purge' 
    hc.sql("""{}""".format(sql))
    
    sql = 'create table ubi.trips as select distinct trip_candidates.* \
    from guobiao_tsp_tbls.trip_candidates inner join guobiao_tsp_tbls.trip_distance \
    on trip_candidates.vin = trip_distance.vin and trip_candidates.start_time = trip_distance.start_time and trip_candidates.distance = trip_distance.distance \
    where trip_candidates.distance % 5 = 0 and trip_distance.distance % 5 = 0'
    hc.sql("""{}""".format(sql))

    sql = 'insert into table ubi.trips select distinct trip_candidates.* \
    from guobiao_tsp_tbls.trip_candidates inner join guobiao_tsp_tbls.trip_distance \
    on trip_candidates.vin = trip_distance.vin and trip_candidates.start_time = trip_distance.start_time and trip_candidates.distance = trip_distance.distance \
    where trip_candidates.distance % 5 = 1 and trip_distance.distance % 5 = 1'
    hc.sql("""{}""".format(sql))
    
    sql = 'insert into table ubi.trips select distinct trip_candidates.* \
    from guobiao_tsp_tbls.trip_candidates inner join guobiao_tsp_tbls.trip_distance \
    on trip_candidates.vin = trip_distance.vin and trip_candidates.start_time = trip_distance.start_time and trip_candidates.distance = trip_distance.distance \
    where trip_candidates.distance % 5 = 2 and trip_distance.distance % 5 = 2'
    hc.sql("""{}""".format(sql))
    
    sql = 'insert into table ubi.trips select distinct trip_candidates.* \
    from guobiao_tsp_tbls.trip_candidates inner join guobiao_tsp_tbls.trip_distance \
    on trip_candidates.vin = trip_distance.vin and trip_candidates.start_time = trip_distance.start_time and trip_candidates.distance = trip_distance.distance \
    where trip_candidates.distance % 5 = 3 and trip_distance.distance % 5 = 3'
    hc.sql("""{}""".format(sql))
    
    sql = 'insert into table ubi.trips select distinct trip_candidates.* \
    from guobiao_tsp_tbls.trip_candidates inner join guobiao_tsp_tbls.trip_distance \
    on trip_candidates.vin = trip_distance.vin and trip_candidates.start_time = trip_distance.start_time and trip_candidates.distance = trip_distance.distance \
    where trip_candidates.distance % 5 = 4 and trip_distance.distance % 5 = 4'
    hc.sql("""{}""".format(sql))    
    
    print("============================")
    print("=======trips================")
    print("=========created============")
    print("============================")
    
    sql = 'drop table if exists ubi.trip_distance_complete purge' 
    hc.sql("""{}""".format(sql))
    
    sql = 'create table ubi.trip_distance_complete as select vin, end_time, min(start_time) as start_time \
    from ubi.trips group by vin, end_time'
    hc.sql("""{}""".format(sql))
            
    print("============================")
    print("===trip_distance_complete===")
    print("=========created============")
    print("============================")
    
    print('End')



def BatchGenerateTrips(sc):
    
    sql = 'DROP TABLE IF EXISTS guobiao_tsp_tbls.starts_redundant PURGE' 
    hc.sql("""{}""".format(sql))

    sql = 'create table guobiao_tsp_tbls.starts_redundant as select vin, loc_lat, loc_lon, bigint(ts / 1000) as ts_seconds, day, veh_odo from \
        guobiao_tsp_tbls.guobiao_raw_orc where veh_st = 1'
    hc.sql("""{}""".format(sql))

    sql = 'DROP TABLE IF EXISTS guobiao_tsp_tbls.ends_redundant PURGE' 
    hc.sql("""{}""".format(sql))

    sql = 'create table guobiao_tsp_tbls.ends_redundant as select vin, loc_lat, loc_lon, bigint(ts / 1000) as ts_seconds, day, veh_odo \
        from guobiao_tsp_tbls.guobiao_raw_orc where veh_st = 2'
    hc.sql("""{}""".format(sql))
    
    GenerateTrips(sc)
    
    sql = 'drop table if exists ubi.trips_complete purge' 
    hc.sql("""{}""".format(sql))
    
    sql = 'create table ubi.trips_complete as select distinct trips.* \
    from ubi.trips inner join ubi.trip_distance_complete \
    on trips.vin = trip_distance_complete.vin and trips.start_time = trip_distance_complete.start_time and trips.end_time = trip_distance_complete.end_time'


# def BatchGenerateTrips(sc):
    
    # sql = 'drop table if exists ubi.trips_complete purge' 
    # hc.sql("""{}""".format(sql))
    
    # sql = 'create table ubi.trips_complete as select distinct trips.* \
    # from ubi.trips inner join ubi.trip_distance_complete \
    # on trips.vin = trip_distance_complete.vin and trips.start_time = trip_distance_complete.start_time and trips.end_time = trip_distance_complete.end_time \
    # where int(trips.start_time) % 5 = 0'
    
    # hc.sql("""{}""".format(sql))
    
    # sql = 'insert into table ubi.trips_complete select distinct trips.* \
    # from ubi.trips inner join ubi.trip_distance_complete \
    # on trips.vin = trip_distance_complete.vin and trips.start_time = trip_distance_complete.start_time and trips.end_time = trip_distance_complete.end_time \
    # where int(trips.start_time) % 5 = 1'
    
    # hc.sql("""{}""".format(sql))
    
    # sql = 'insert into table ubi.trips_complete select distinct trips.* \
    # from ubi.trips inner join ubi.trip_distance_complete \
    # on trips.vin = trip_distance_complete.vin and trips.start_time = trip_distance_complete.start_time and trips.end_time = trip_distance_complete.end_time \
    # where int(trips.start_time) % 5 = 2'

    # hc.sql("""{}""".format(sql))
    
    # sql = 'insert into table ubi.trips_complete select distinct trips.* \
    # from ubi.trips inner join ubi.trip_distance_complete \
    # on trips.vin = trip_distance_complete.vin and trips.start_time = trip_distance_complete.start_time and trips.end_time = trip_distance_complete.end_time \
    # where int(trips.start_time) % 5 = 3'
    # hc.sql("""{}""".format(sql))
    
    # sql = 'insert into table ubi.trips_complete select distinct trips.* \
    # from ubi.trips inner join ubi.trip_distance_complete \
    # on trips.vin = trip_distance_complete.vin and trips.start_time = trip_distance_complete.start_time and trips.end_time = trip_distance_complete.end_time \
    # where int(trips.start_time) % 5 = 4'

    # hc.sql("""{}""".format(sql))


#def TestTodayTrips(sc, n):
#    start_date = date(2018, 11, 7)- timedelta(n)
#    end_date = date(2018, 11, 7)
#    sql="""create table guobiao_tsp_tbls.end_time as select vin, max(bigint(ts / 1000)) as lasttime from guobiao_tsp_tbls.guobiao_raw_orc \
#       where day='{}' and veh_st = 2 group by vin""".format(start_date.strftime("%Y%m%d")) 
#    print(sql)    

def GenerateTodayTrips(sc, n):
    start_date = date.today() - timedelta(n)
    end_date = date.today()
    #start_date = date(2018, 11, 7)- timedelta(n)
    #end_date = date(2018, 11, 7)
    print("Starting calculating from " + str(start_date))

    sql = 'DROP TABLE IF EXISTS guobiao_tsp_tbls.end_time PURGE'  
    hc.sql("""{}""".format(sql))   
    sql="""create table guobiao_tsp_tbls.end_time as select vin, max(bigint(ts / 1000)) as lasttime from guobiao_tsp_tbls.guobiao_raw_orc \
       where day='{}' and veh_st = 2 group by vin""".format(start_date.strftime("%Y%m%d")) 
    print(sql)
    hc.sql("""{}""".format(sql))
    
    print("============================")
    print("=======end_time=============")
    print("=========created============")
    print("============================")
    
    sql = 'DROP TABLE IF EXISTS guobiao_tsp_tbls.starts_redundant PURGE' 
    hc.sql("""{}""".format(sql))

    sql = """create table guobiao_tsp_tbls.starts_redundant as select a.vin, a.loc_lat, a.loc_lon, bigint(a.ts / 1000) as ts_seconds, a.day, a.veh_odo from \
       guobiao_tsp_tbls.guobiao_raw_orc a inner join guobiao_tsp_tbls.end_time b on a.vin = b.vin where a.day>='{}' and a.veh_st = 1 and bigint(a.ts / 1000) > b.lasttime """.format(start_date.strftime("%Y%m%d"))
    hc.sql("""{}""".format(sql))
    
    print("============================")
    print("=======starts_redundant=====")
    print("=========created============")
    print("============================")

    sql = 'DROP TABLE IF EXISTS guobiao_tsp_tbls.ends_redundant PURGE' 
    hc.sql("""{}""".format(sql))

    sql = """create table guobiao_tsp_tbls.ends_redundant as select vin, loc_lat, loc_lon, bigint(ts / 1000) as ts_seconds, day, veh_odo \
        from guobiao_tsp_tbls.guobiao_raw_orc where day='{}' and veh_st = 2""".format(end_date.strftime("%Y%m%d"))
    hc.sql("""{}""".format(sql))
    
    print("============================")
    print("=======ends_redundant=======")
    print("=========created============")
    print("============================")
    
    GenerateTrips(sc)
        
    sql = 'insert into table ubi.trips_complete select distinct trips.* \
    from ubi.trips inner join ubi.trip_distance_complete \
    on trips.vin = trip_distance_complete.vin and trips.start_time = trip_distance_complete.start_time and trips.end_time = trip_distance_complete.end_time'
    hc.sql("""{}""".format(sql)) 
    
    print("============================")
    print("=======trips_complete=======")
    print("=========Inserted===========")
    print("============================")
	
	
def GenerateTodayTrips2(sc, n):
    start_date = date.today() - timedelta(n)
    end_date = date.today()
    #start_date = date(2019, 6, 30) - timedelta(n)
    #end_date = date(2019, 6, 30) 
    #start_date = date(2018, 11, 8)- timedelta(n)
    #end_date = date(2018, 11, 8)
    print("Starting calculating from " + str(start_date))

    sql = 'DROP TABLE IF EXISTS guobiao_tsp_tbls.end_time PURGE'  
    hc.sql("""{}""".format(sql))   
    sql="""create table guobiao_tsp_tbls.end_time as select vin, max(bigint(ts / 1000)) as lasttime from guobiao_tsp_tbls.guobiao_raw_orc \
       where day='{}' and veh_st = 2 group by vin""".format(start_date.strftime("%Y%m%d")) 
    print(sql)
    hc.sql("""{}""".format(sql)); logger.info(sql)
    
    print("============================")
    print("=======end_time=============")
    print("=========created============")
    print("============================")

    sql = 'DROP TABLE IF EXISTS guobiao_tsp_tbls.duplicates PURGE' 
    hc.sql("""{}""".format(sql)); logger.info(sql)

    sql = """create table guobiao_tsp_tbls.duplicates as select a.vin, a.loc_lat, a.loc_lon, bigint(a.ts / 1000) as ts_seconds, a.day, a.veh_odo from \
       guobiao_tsp_tbls.guobiao_raw_orc a inner join guobiao_tsp_tbls.end_time b on a.vin = b.vin where a.day='{}' and a.veh_st = 1 and bigint(a.ts / 1000) <= b.lasttime """.format(start_date.strftime("%Y%m%d"))
    hc.sql("""{}""".format(sql)); logger.info(sql)

    
    sql = 'DROP TABLE IF EXISTS guobiao_tsp_tbls.starts_redundant PURGE' 
    hc.sql("""{}""".format(sql)); logger.info(sql)

    sql = """create table guobiao_tsp_tbls.starts_redundant as select vin, loc_lat, loc_lon, bigint(ts / 1000) as ts_seconds, day, veh_odo from \
       guobiao_tsp_tbls.guobiao_raw_orc where day<='{0}' and day>='{1}' and veh_st = 1 EXCEPT select * from guobiao_tsp_tbls.duplicates """.format(end_date.strftime("%Y%m%d"),start_date.strftime("%Y%m%d"))
    hc.sql("""{}""".format(sql)); logger.info(sql)
    
    print("============================")
    print("=======starts_redundant=====")
    print("=========created============")
    print("============================")

    sql = 'DROP TABLE IF EXISTS guobiao_tsp_tbls.ends_redundant PURGE' 
    hc.sql("""{}""".format(sql)); logger.info(sql)

    sql = """create table guobiao_tsp_tbls.ends_redundant as select vin, loc_lat, loc_lon, bigint(ts / 1000) as ts_seconds, day, veh_odo \
        from guobiao_tsp_tbls.guobiao_raw_orc where day<='{0}' and day>='{1}' and veh_st = 2""".format(end_date.strftime("%Y%m%d"),start_date.strftime("%Y%m%d"))
    hc.sql("""{}""".format(sql)); logger.info(sql)
    
    print("============================")
    print("=======ends_redundant=======")
    print("=========created============")
    print("============================")
    
    GenerateTrips(sc); logger.info("After calling GenerateTrips(sc)")
        
    sql = 'insert into table ubi.trips_complete select distinct trips.* \
    from ubi.trips inner join ubi.trip_distance_complete \
    on trips.vin = trip_distance_complete.vin and trips.start_time = trip_distance_complete.start_time and trips.end_time = trip_distance_complete.end_time'
    hc.sql("""{}""".format(sql)) ; logger.info(sql)
    
    print("============================")
    print("=======trips_complete=======")
    print("=========Inserted===========")
    print("============================")

    
def hive2string(hc, query, colname):
    spark_df = hc.sql("""{}""".format(query))
    row = spark_df.first()
    res = row[colname]
    return res


def hive2pandas(hc, query):
    spark_df = hc.sql("""{}""".format(query))
    # Convert to pandas dataframe
    df = spark_df.toPandas()
    return df

def hive2csv(hc, query, outfile):
    df = hive2pandas(hc, query)
    df.to_csv(outfile)
    
def generateList(hc, start_day):
    tableName = "ubi.trips_complete"
    query = """SELECT vin, start_time, end_time FROM {} WHERE start_day = '{}'""".format(tableName, start_day)
    df=hive2pandas(hc, query)
    res={}
    for x in range(len(df)):
        vin=df.iloc[x,0]
        start_time=df.iloc[x,1]
        end_time=df.iloc[x,2]
        temp=(start_time, end_time)
        if not res.has_key(vin):
            res.setdefault(vin,[])
        res[vin].append(temp)
    
    with open('/home/wchen/ubi/log_guobiaoxxxxx.txt', 'w') as outfile:
        for vin in res.keys():
            outfile.write(vin + '\t' + str(res[vin]) + '\n')
    return res

#def addLocation(hc, p):
#    query = """SELECT distinct ROUND(CAST(start_loc_lat as float), 3) as loc_lat, ROUND(CAST(start_loc_lon as float), 3) as loc_lon from ubi.trips_complete where start_loc_lat LIKE p union SELECT distinct ROUND(CAST(end_loc_lat as float), 3) as loc_lat, ROUND(CAST(end_loc_lon as float), 3) as loc_lon from ubi.trips_complete where end_loc_lat LIKE p"""
#    df=hive2pandas(hc, query)
#    with open('/home/wchen/ubi/latlon2location_' + p.replace('%', '') + '.txt', 'w') as outfile:
#        for x in range(len(df)):
#            g = geocoder.gaode([df.iloc[x,0], df.iloc[x,1]], method='reverse', key='27522a3d9da8f4e80d6580c80d010d4c')
#            adds = g.address
#            if g.address is None:
#                adds=''
#            elif type(g.address)==list:
#                adds=str(adds)
#            outfile.write('%.3f'%(df.iloc[x,0]) + '\t' + '%.3f'%(df.iloc[x,1]) + '\t' + adds.encode('utf-8') + '\n')
#    print('Done with location: ' + p)
    
def addLocation(hc):
    query = """SELECT distinct CAST(start_loc_lat as float) as loc_lat, CAST(start_loc_lon as float) as loc_lon from ubi.trips_complete union SELECT distinct CAST(end_loc_lat as float) as loc_lat, CAST(end_loc_lon as float) as loc_lon from ubi.trips_complete where
start_day>='20190212' and start_day<='20190313'"""
    df=hive2pandas(hc, query)
    with open('/home/wchen/dsc/latlon2location_0312' + '.txt', 'w') as outfile:
        for x in range(len(df)):
            g = geocoder.gaode([df.iloc[x,0], df.iloc[x,1]], method='reverse', key='27522a3d9da8f4e80d6580c80d010d4c')
            adds = g.address
            if g.address is None:
                adds=''
            elif type(g.address)==list:
                adds=str(adds)
            outfile.write(df.iloc[x,0] + '\t' + df.iloc[x,1] + '\t' + adds.encode('utf-8') + '\n')
    print('Done with location!')

from pyspark.sql.types import StructType,StructField,DoubleType,StringType

def csv2hive(hc, infile):
    print(infile)
    if os.path.isfile(infile):
        df = pd.read_csv(infile, sep='\t', encoding="utf-8")
        print(infile)
        df.head()
        mySchema = StructType([StructField("loc_latitude", DoubleType(), True)\
                       ,StructField("loc_longtitude", DoubleType(), True)\
                       ,StructField("address", StringType(), True)])
        #spark_df = hc.createDataFrame([tuple(x for x in record.tolist()) for record in df.to_records(index=False)], df.columns.tolist())
        spark_df = hc.createDataFrame(df, mySchema)
        cols = [when(~col(x).isin("NULL", "NA", "NaN",""), col(x)).alias(x) for x in spark_df.columns]
        spark_df = spark_df.select(*cols)
        spark_df.registerTempTable('update_dataframe')
        sql_cmd = """INSERT INTO TABLE ubi.address SELECT loc_latitude, loc_longtitude, address FROM update_dataframe"""
        print(sql_cmd)
	hc.sql(sql_cmd)
	print('Table address creation done.')
                
            
if __name__ == "__main__":

    root_logger = logging.getLogger("")
    logger_name = "trip_v3"
    logger = logging.getLogger(logger_name)
    assign_log_handler(root_logger=root_logger, log_file='/home/wchen/dsc/logging.dat')
    logger.info("\n\n")

#PrepareTodayData(sc) or BatchPrepareData(sc)
    args = sys.argv[1:]
    
    usage_msg = """usage:
        \tfor today: --today 
        \tto run entire history: --all 
        \tfor a start date: -d yyyymmdd or -date yyyymmdd
        \tto get help: --help"""

    help_msg = """Required to specify all days or today trip data.
        Options:
        \t-today: add today's trip
        \t--all: run for entire history, e.g. --all
        \t-d, --date: specify a starting date, e.g. -d 20150118"""
    if not args:
        print(usage_msg)
        sys.exit(1)
        
    # run one day
    if args[0] == '--today':  
        if len(args) < 1:
            print('invalid input')
            sys.exit(1)
        #if len(args[1]) != 8:
         #   print('invalid input: {}'.format(args[1]))
         #   sys.exit(1)
        #GenerateTodayTrips2(sc, 39)11/8-12/17   12/17-12/28(11)  12/28-01/14 (17)  1/14-2/11(28) 2/11-2/22(11) 2/22-3/13(19)no3/13 available  3/12-3/13(1)  3/13-3/31(18)  3/31-4/30(30)  4/30-5/31(31) 5/31-6/30(30)  6/30-7/16(16)
        GenerateTodayTrips2(sc, 16); logger.info("I would not see this line")
	#TestTodayTrips(sc, 1)
        del args[0]

        
    # run entire history
    elif args[0] == '--all':  
        #BatchGenerateTrips(sc)
        del args[0]
        
    #  return result in dictionary
    elif args[0] in ['-d', '--date']:  # run one day
        if len(args) < 2:
            print('no date provided')
            sys.exit(1)
        if len(args[1]) != 8:
            print('invalid input date: {}'.format(args[1]))
            sys.exit(1)
        generateList(hc, args[1])
        del args[0:2]
        
    elif args[0] == '--help':
        print(help_msg)
        print(usage_msg)
        sys.exit(0)
        
    # Get location records
    elif args[0] == '--location':
        #addLocation(hc, '%1')
        addLocation(hc)
        del args[0]
    
    #Create and upload address table
    elif args[0] == '--upload':
        csv2hive(hc, args[1])
        del args[0:2]
        
    else:
        print('invalid option {}'.format(args[0]))
        sys.exit(1)
    # generateList(hc, '20170517')
