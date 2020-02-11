from datetime import datetime
from datetime import date, timedelta
import time
import sys
import random
from StringIO import StringIO
from collections import OrderedDict 
import itertools as it

import findspark
findspark.init('/usr/hdp/current/spark2-client')

import pyspark
from pyspark.sql.functions import col

from sys import path
path.insert(0, '/home/prod/git/common/collect_raw_data_utils')
from data_collection import *
path.insert(0, '/home/prod/git/common/pyspark_utils')
from get_spark import get_spark
SPARK_CONFIG_LIST = [
    ('spark.app.name', 'get_raw_data'),           # spark job name
    ('spark.yarn.queue', 'prod'),                          # for production code, use 'prod', else use 'dev'
    ('spark.master', 'yarn'),                              # choose between 'yarn', 'local', or 'local[*]'
    ('spark.submit.deployMode', 'client'),                 # 'client' or 'cluster' mode
    ('spark.yarn.am.memory', '10g'),                       # memory for spark driver (application master), for client mode
    ('spark.executor.memory', '5g'),                       # memory for each executor
    ('spark.executor.cores', '5'),                         # max number of cores each executor can use (means number of tasks)
    ('spark.executor.instances', '5'),                    # max number of executors
]


spark = get_spark(SPARK_CONFIG_LIST)
sc = spark.sparkContext

hc = HiveContext(sc)

num_cells = 90
num_modules = 40
vintype = ['A5HEV']
sel_col = ['day', 'vin', 'ts']
sel_col += ["veh_charge_st", "veh_curr", "esd_sc_temp_list"]# "esd_sc_volt_list"]

def read_data(date_to_check): 
    stime = time.time()  
    raw_spark_df = get_raw_records_in_spark(hc, 
                                        vin_types=vintype,
                                       # vins=vins,
                                        record_times=date_to_check, 
                                        sel_cols=sel_col, 
                                        nrows_to_print=None)
    print("Get raw records in spark done!")
    raw_spark_df = raw_spark_df.repartition(1000, "vin")
    
    casted_columns = ['day', 'vin', col('ts').cast("long").alias('ts'), 'record_time',
                    col("veh_curr").cast("float").alias("veh_curr"),
                   #  col("veh_odo").cast("float").alias("veh_odo"),
                     col("veh_charge_st").cast("short").alias("veh_charge_st"),
                   #  col("veh_st").cast("int").alias("veh_st"),
                   "esd_sc_temp_list",# "esd_sc_volt_list", 
                   ]
    raw_spark_df = raw_spark_df.select(*casted_columns)
    raw_spark_df = convert_signal_str_to_col_use_spark(
        raw_spark_df,
        cols_split_dict={
            'esd_sc_temp_list':("temp", num_modules, "\|", "int"),
            #'esd_sc_volt_list':("volt", num_cells, "\|", "float")
        },
        other_cols=[c for c in raw_spark_df.columns if c not in ["esd_sc_temp_list"]]# "esd_sc_volt_list"]]
    )
    print("Split columns done!")
    
    raw_spark_df.drop(*["record_time"])
    print("drop column done!")
    
    print(raw_spark_df.show(5))

    vins = raw_spark_df.select("vin").distinct().collect()
    vins = [row.vin for row in vins]
    #vin_sample = random.sample(list_of_vins, 100)
    vin_sample = vins
    #vin_sample = random.sample(vins, 100)
    num_vins = len(vin_sample)
    print("num of vins is " + str(num_vins))

    df_list = []
    num_vin_to_process = 10

    for ind in range(0, num_vins, num_vin_to_process):
        print("Process data from " + str(ind) + " to " + str(ind+num_vin_to_process))
        short_vin_list = vins[ind:(ind+num_vin_to_process)]
        slice_spark = raw_spark_df[raw_spark_df.vin.isin(short_vin_list)]
        vins_df = slice_spark.toPandas()

        # to reduce memory usage because of conversion from spark df to pandas df
        df_as_str = vins_df.to_csv(path_or_buf=None, index=False)
        del vins_df
        vins_df = pd.read_csv(StringIO(df_as_str), sep=",")
        del df_as_str
        df_list.append(vins_df)

    if len(df_list) > 0:
        raw_data_df = pd.concat(df_list)

    raw_spark_df.unpersist()
    del raw_spark_df
    
    #raw_spark_df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("20191120.csv")
    print("Converting to Pandas done!")
    
    etime = time.time()
    print "Finished in {} seconds".format(etime - stime)
    
    return raw_data_df

start_date = date(2020, 1, 15)
#start_date = date.today()
end_date = date(2020, 1, 1)
def daterange(start_date, end_date):
    for n in range(int((start_date - end_date).days + 1)):
        yield start_date - timedelta(n)
for single_date in daterange(start_date, end_date):
    dates.append(single_date.strftime('%Y%m%d'))

for day in dates:
    df = read_data(day)
    df.to_csv("df_" + day +".csv")
    all_temp_and_curr = df.drop(['record_time'], axis=1)
    fast_charging_mask = (all_temp_and_curr['veh_curr']) < -30 & (all_temp_and_curr['veh_charge_st'] == 1)
    temp_fast_charging = all_temp_and_curr[fast_charging_mask]
    temp_fast_charging = temp_fast_charging.drop(['veh_curr', 'veh_charge_st'], axis=1)
    temp_fast_charging.to_csv("temp_" + day + "_all_fast_charging.csv")
    temp_fast_charging['ts'] = convert_ts_to_datetime(temp_fast_charging['ts'])
    fast_charging_df_2 = temp_fast_charging
    array_of_vins_2 = fast_charging_df_2.vin.unique()
    list_of_vins_2 = array_of_vins_2.tolist()

    dic = OrderedDict() 
    i = 0
    rate_list = []       # get all the rates for histogram
    suspect_vin = set()
    rate_to_check = 6
    anomaly_to_check = 2

    temperature_list = ["temp_" + str(s) for s in range(1,41)]

    result_list_5 = []

    for vin in list_of_vins_2:
        print("Process " + vin + ":")
        vindf = xyz[xyz.vin == vin]

        vin_df = vindf.reset_index()

        i = i + 1

        vin_df['ts'] = vin_df['ts'].dt.tz_localize(None)
        vin_df = vin_df.sort_values(by='ts')

        vin_df = vin_df.set_index(["ts"], inplace = False)

        ts_diff_5 = pd.Series(vin_df.index, index=vin_df.index).diff(periods=30).iloc[1:].dt.seconds.div(60, fill_value=0)
        temp_diff_5 = vin_df[temperature_list].mean(axis=1).diff(periods=30).iloc[1:]
        rate_df_5 = temp_diff_5.div(ts_diff_5, axis=0)
        rate_df_5_subset = rate_df_5[rate_df_5 > 1]

        if len(rate_df_5_subset) != 0:
            suspect_vin.add(str(vin)+"_"+str(rate_df_5_subset.idxmax())+"_"+str(rate_df_5_subset.max())+"_"+str(temp_diff_5[rate_df_5_subset.idxmax()])
                           +"_"+str(ts_diff_5[rate_df_5_subset.idxmax()]))
        result_list_5.append(rate_df_5_subset)

        print("Vin #" + str(i) + ":" + vin + ", the length of changing rate is " + str(len(result_list_5)))

    with open('suspect.txt', 'aw') as f:
        for item in suspect_vin:
            print >> f, item

