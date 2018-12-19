# -*- coding: utf-8 -*-
#DO NOT CHANGE#

from os.path import exists
import pandas as pd
import numpy as np
import time
import findspark  # find spark home directory
import datetime
import pytz
import sys
# Plotting
import matplotlib.pyplot as plt
import seaborn as sns
from dateutil.relativedelta import relativedelta


# Configure OPTIONS
findspark.init('/usr/hdp/current/spark2-client')
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext

def get_Spark():

    conf = pyspark.SparkConf().setAll([
        ('spark.submit.deployMode', 'client'), # deploy in yarn-client or yarn-cluster
        ('spark.executor.memory', '8g'),       # memory allocated for each executor
        ('spark.executor.cores', '3'),         # number of cores for each executor
        ('spark.executor.instances', '10'),    # number of executors in total
        ('spark.yarn.am.memory', '10g')])       # memory for spark driver (application master)
    spark = SparkSession.builder \
    .master("yarn") \
    .appName("ubi_trip") \
    .enableHiveSupport() \
    .config(conf = conf) \
    .getOrCreate()

    return spark
    
spark = get_Spark()
spark_context = spark.sparkContext
hc = HiveContext(spark_context)

import pandas as pd
import numpy as np

MAXDATE=now.strftime("%Y%m%d")

def _generate_query(s_date="20170517", e_date=MAXDATE, vins=[], cols=[]):
    table = 'guobiao_tsp_tbls.guobiao_raw_orc'
    query = ''
    if cols is None or len(cols) == 0:
        query += "SELECT * FROM {}".format(table)
    else:
        query += "SELECT {} FROM {}".format(",".join(cols+["vin", "vintype", "ts", "veh_st", "veh_charge_st", "veh_runmode"]), table)
    
    next_key_word = "WHERE" if "WHERE" not in query else "AND"
    if (vins is not None) and len(vins) == 1:
        query += "{} vin = '{}'\n".format(next_key_word,vins[0])
    elif (vins is not None) and len(vins) > 1:
        query += "{} vin in {}\n".format(next_key_word,tuple(vins))
    else:
        pass
        
    if(s_date is not None and len(s_date)==8):
        if "WHERE" not in query:
            query += "WHERE "
        else:
            query += "AND " 
        query += "day >= '{}' ".format(str(s_date))

    if(e_date is not None and len(e_date)==8):
        if "WHERE" not in query:
            query += "WHERE "
        else:
            query += "AND " 
        query += "day <= '{}' ".format(str(e_date))
    
    return query

def trip_generator(hc, s_date="20170517", e_date=MAXDATE, vins=[], cols=[],
    num_vin_to_process = 100):
    
    query = _generate_query(s_date, e_date, vins, cols)
    
    raw_data_df = pd.DataFrame()
    if query:
        print("""submitting query \n{}""".format(query))
        try:
            spark_df = hc.sql("""SELECT a.*, b.start_time, b.end_time FROM ({}) a JOIN ubi.trips_complete b ON a.vin = b.vin WHERE from_utc_timestamp(to_utc_timestamp(from_unixtime(bigint(a.ts / 1000)), "America/Los_Angeles"), "Asia/Shanghai") >= b.start_time AND from_utc_timestamp(to_utc_timestamp(from_unixtime(bigint(a.ts / 1000)), "America/Los_Angeles"), "Asia/Shanghai") <= b.end_time """.format(query))
        except Exception as error:
            print("Invalid query, check error msg: {}".format(error))
        else:
            spark_df.persist()
            vins = spark_df.select("vin").distinct().collect()
            
            for vin in [row.vin for row in vins]:
                slice_spark = spark_df.loc[spark_df.vin == vin]

                starts = slice_spark.select("start_time").distinct().collect()
                df_list = []
                for st in [row.start_time for row in starts]:
                    trip_df = slice_spark.loc[slice_spark.start_time == st]
                    df_list.append(trip_df)
                yield (vin, df_list)
                
            spark_df.unpersist()
            del spark_df
        
    else:
        print("{}, abort querying Hive".format(msg))

                  
if __name__ == "__main__":
    args = sys.argv[1:]
    if args[0] == '--DataFrame':  
		number=0
        for value in trip_generator(hc, s_date="20170517", e_date=MAXDATE, [], []):
			print(value)
			number=number+1
			if number=1:
				return
