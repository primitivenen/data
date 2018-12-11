import sys
import os
import geocoder

# Build spark session
import findspark

# spark location on namenode server
findspark.init("/usr/hdp/current/spark2-client")
import pyspark
conf = pyspark.SparkConf().setAll([('spark.app.name', 'guobiao_tsp_tbls.trip_map'), # App Name
    ('spark.master', 'yarn'),              # spark run mode: locally or remotely
    ('spark.submit.deployMode', 'client'), # deploy in yarn-client or yarn-cluster
    ('spark.executor.memory', '10g'),       # memory allocated for each executor
    #('spark.memory.fraction', '0.7'),
    ('spark.executor.cores', '3'),         # number of cores for each executor
    ('spark.executor.instances', '5'),    # number of executors in total
    ('spark.driver.maxResultSize', '5g'), # Result size is large, need to increase from default of 1g
    ('spark.yarn.am.memory', '10g')])       # memory for spark driver (application master)
sc = pyspark.SparkContext.getOrCreate(conf=conf)

from pyspark.sql import HiveContext

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

    sql = 'DROP TABLE IF EXISTS guobiao_tsp_tbls.starts PURGE' 
    hc.sql("""{}""".format(sql))

    sql = 'create table guobiao_tsp_tbls.starts as select starts_redundant.* from \
        guobiao_tsp_tbls.starts_redundant inner join guobiao_tsp_tbls.starts_latest \
        on starts_redundant.vin = starts_latest.vin and starts_redundant.veh_odo == starts_latest.veh_odo and starts_redundant.ts_seconds = starts_latest.ts_seconds'
    hc.sql("""{}""".format(sql))

    
    sql = 'DROP TABLE IF EXISTS guobiao_tsp_tbls.ends_latest PURGE' 
    hc.sql("""{}""".format(sql))
    
    sql = 'create table guobiao_tsp_tbls.ends_latest as select vin, veh_odo, min(ts_seconds) as ts_seconds \
    from guobiao_tsp_tbls.ends_redundant group by vin, veh_odo'
    hc.sql("""{}""".format(sql))
    
    sql = 'DROP TABLE IF EXISTS guobiao_tsp_tbls.ends PURGE' 
    hc.sql("""{}""".format(sql))

    sql = 'create table guobiao_tsp_tbls.ends as select ends_redundant.* \
        from guobiao_tsp_tbls.ends_redundant inner join guobiao_tsp_tbls.ends_latest \
        on ends_redundant.vin = ends_latest.vin and ends_redundant.veh_odo == ends_latest.veh_odo \
        and ends_redundant.ts_seconds = ends_latest.ts_seconds'
    hc.sql("""{}""".format(sql))
    
    sql = 'drop table if exists guobiao_tsp_tbls.trip_candidates purge' 
    hc.sql("""{}""".format(sql))
    
    sql = 'create table guobiao_tsp_tbls.trip_candidates as select starts.vin, starts.loc_lat as start_loc_lat, starts.loc_lon as start_loc_lon, \
    from_utc_timestamp(to_utc_timestamp(from_unixtime(starts.ts_seconds), "America/Los_Angeles"), "Asia/Shanghai") as start_time, \
    starts.day as start_day, ends.loc_lat as end_loc_lat, ends.loc_lon as end_loc_lon, from_utc_timestamp(to_utc_timestamp(from_unixtime(ends.ts_seconds), "America/Los_Angeles"), "Asia/Shanghai") as end_time, \
    ends.veh_odo - starts.veh_odo as distance \
    from guobiao_tsp_tbls.starts inner join guobiao_tsp_tbls.ends on starts.vin = ends.vin \
    where ends.veh_odo >= starts.veh_odo and ends.ts_seconds > starts.ts_seconds and ends.ts_seconds - starts.ts_seconds < 50000 and ends.veh_odo - starts.veh_odo < 1000'
    hc.sql("""{}""".format(sql))

    sql = 'drop table if exists guobiao_tsp_tbls.trip_distance purge' 
    hc.sql("""{}""".format(sql))
    
    sql = 'create table guobiao_tsp_tbls.trip_distance as select vin, start_time, min(distance) as distance \
    from guobiao_tsp_tbls.trip_candidates group by vin, start_time'
    hc.sql("""{}""".format(sql))

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
    
    sql = 'drop table if exists ubi.trip_distance_complete purge' 
    hc.sql("""{}""".format(sql))
    
    sql = 'create table ubi.trip_distance_complete as select vin, end_time, min(start_time) as start_time \
    from ubi.trips group by vin, end_time'
    hc.sql("""{}""".format(sql))
            
    print('End')



# def BatchGenerateTrips(sc):
#     
#     sql = 'DROP TABLE IF EXISTS guobiao_tsp_tbls.starts_redundant PURGE' 
#     hc.sql("""{}""".format(sql))
# 
#     sql = 'create table guobiao_tsp_tbls.starts_redundant as select vin, loc_lat, loc_lon, bigint(ts / 1000) as ts_seconds, day, veh_odo from \
#         guobiao_tsp_tbls.guobiao_raw_orc where veh_st = 1'
#     hc.sql("""{}""".format(sql))
# 
#     sql = 'DROP TABLE IF EXISTS guobiao_tsp_tbls.ends_redundant PURGE' 
#     hc.sql("""{}""".format(sql))
# 
#     sql = 'create table guobiao_tsp_tbls.ends_redundant as select vin, loc_lat, loc_lon, bigint(ts / 1000) as ts_seconds, day, veh_odo \
#         from guobiao_tsp_tbls.guobiao_raw_orc where veh_st = 2'
#     hc.sql("""{}""".format(sql))
#     
#     GenerateTrips(sc)
#     
#     sql = 'drop table if exists ubi.trips_complete purge' 
#     hc.sql("""{}""".format(sql))
#     
#     sql = 'create table ubi.trips_complete as select distinct trips.* \
#     from ubi.trips inner join ubi.trip_distance_complete \
#     on trips.vin = trip_distance_complete.vin and trips.start_time = trip_distance_complete.start_time and trips.end_time = trip_distance_complete.end_time'


def BatchGenerateTrips(sc):
    
    sql = 'drop table if exists ubi.trips_complete purge' 
    hc.sql("""{}""".format(sql))
    
    sql = 'create table ubi.trips_complete as select distinct trips.* \
    from ubi.trips inner join ubi.trip_distance_complete \
    on trips.vin = trip_distance_complete.vin and trips.start_time = trip_distance_complete.start_time and trips.end_time = trip_distance_complete.end_time \
    where int(trips.start_time) % 5 = 0'
    
    hc.sql("""{}""".format(sql))
    
    sql = 'insert into table ubi.trips_complete select distinct trips.* \
    from ubi.trips inner join ubi.trip_distance_complete \
    on trips.vin = trip_distance_complete.vin and trips.start_time = trip_distance_complete.start_time and trips.end_time = trip_distance_complete.end_time \
    where int(trips.start_time) % 5 = 1'
    
    hc.sql("""{}""".format(sql))
    
    sql = 'insert into table ubi.trips_complete select distinct trips.* \
    from ubi.trips inner join ubi.trip_distance_complete \
    on trips.vin = trip_distance_complete.vin and trips.start_time = trip_distance_complete.start_time and trips.end_time = trip_distance_complete.end_time \
    where int(trips.start_time) % 5 = 2'

    hc.sql("""{}""".format(sql))
    
    sql = 'insert into table ubi.trips_complete select distinct trips.* \
    from ubi.trips inner join ubi.trip_distance_complete \
    on trips.vin = trip_distance_complete.vin and trips.start_time = trip_distance_complete.start_time and trips.end_time = trip_distance_complete.end_time \
    where int(trips.start_time) % 5 = 3'
    hc.sql("""{}""".format(sql))
    
    sql = 'insert into table ubi.trips_complete select distinct trips.* \
    from ubi.trips inner join ubi.trip_distance_complete \
    on trips.vin = trip_distance_complete.vin and trips.start_time = trip_distance_complete.start_time and trips.end_time = trip_distance_complete.end_time \
    where int(trips.start_time) % 5 = 4'

    hc.sql("""{}""".format(sql))
    

def GenerateTodayTrips(sc, n):
    from datetime import datetime, date, timedelta
    start_date = date.today() - timedelta(n)
    end_date = date.today()

    sql = 'DROP TABLE IF EXISTS ubi.end_time PURGE'  
    hc.sql("""{}""".format(sql))   
    sql="""create table ubi.end_time as select vin, max(bigint(ts / 1000)) as lasttime from guobiao_tsp_tbls.guobiao_raw_orc where day='{0}' and veh_st = 2 group by vin""".format(start_date) 
    hc.sql("""{}""".format(sql))
    sql = 'DROP TABLE IF EXISTS guobiao_tsp_tbls.starts_redundant PURGE' 
    hc.sql("""{}""".format(sql))

    sql = """create table guobiao_tsp_tbls.starts_redundant as select vin, loc_lat, loc_lon, bigint(ts / 1000) as ts_seconds, day, veh_odo from \
        guobiao_tsp_tbls.guobiao_raw_orc inner join ubi.end_time on guobiao_tsp_tbls.guobiao_raw_orc.vin = ubi.end_time.vin where day>='{0}' and veh_st = 1 and bigint(ts / 1000) > ubi.end_time.lasttime """.format(start_date)
    hc.sql("""{}""".format(sql))

    sql = 'DROP TABLE IF EXISTS guobiao_tsp_tbls.ends_redundant PURGE' 
    hc.sql("""{}""".format(sql))

    sql = """create table guobiao_tsp_tbls.ends_redundant as select vin, loc_lat, loc_lon, bigint(ts / 1000) as ts_seconds, day, veh_odo \
        from guobiao_tsp_tbls.guobiao_raw_orc where day='{0}' and veh_st = 2""".format(end_date)
    hc.sql("""{}""".format(sql))
    
    sql = 'DROP TABLE IF EXISTS ubi.end_time PURGE'     
    sql="""create table ubi.end_time as select vin, max(end_time) from guobiao_tsp_tbls.guobiao_raw_orc where day='{0}' and veh_st = 2 group by vin""".format(start_date) 
    hc.sql("""{}""".format(sql)) 
    
    GenerateTrips(sc)
        
    sql = 'insert into table ubi.trips_complete select distinct trips.* \
    from ubi.trips inner join ubi.trip_distance_complete \
    on trips.vin = trip_distance_complete.vin and trips.start_time = trip_distance_complete.start_time and trips.end_time = trip_distance_complete.end_time'

    
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

def addLocation(hc, p):
    query = """SELECT distinct ROUND(CAST(start_loc_lat as float), 3) as loc_lat, ROUND(CAST(start_loc_lon as float), 3) as loc_lon from ubi.trips_complete union SELECT distinct ROUND(CAST(end_loc_lat as float), 3) as loc_lat, ROUND(CAST(end_loc_lon as float), 3) as loc_lon from ubi.trips_complete"""
    df=hive2pandas(hc, query)
    with open('/home/wchen/ubi/latlon2location_' + str(p) + '.txt', 'w') as outfile:
        for x in range(len(df)):
            if(int(df.iloc[x,0] * 10) % 10 == p):
                g = geocoder.gaode([df.iloc[x,0], df.iloc[x,1]], method='reverse', key='27522a3d9da8f4e80d6580c80d010d4c')
                adds = g.address
                if g.address is None:
                    adds=''
                elif type(g.address)==list:
                    adds=str(adds)
                outfile.write('%.3f'%(df.iloc[x,0]) + '\t' + '%.3f'%(df.iloc[x,1]) + '\t' + adds.encode('utf-8') + '\n')
    print('Done with location: ' + p)
        
if __name__ == "__main__":
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
    if args[0] == '--today':  # run one day
        if len(args) < 2:
            print('no date provided')
            sys.exit(1)
        if len(args[1]) != 8:
            print('invalid input date: {}'.format(args[1]))
            sys.exit(1)
        #GenerateTodayTrips(sc, 1)
        del args[0]
    elif args[0] == '--all':  # run entire history
        #BatchGenerateTrips(sc)
        del args[0]
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
    elif args[0] == '--location':
        addLocation(hc, 7)
        del args[0]
    else:
        print('invalid option {}'.format(args[0]))
        sys.exit(1)
    # generateList(hc, '20170517')
