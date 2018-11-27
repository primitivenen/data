from datetime import datetime, date
import subprocess
import pandas as pd
import os
import sys
import findspark   # find spark home directory
findspark.init("/usr/hdp/current/spark2-client")   # spark location on namenode server

import numpy as np
from datetime import timedelta
import pyspark
from pyspark.sql import HiveContext
from pyspark.sql.functions import col, when

from pyspark.sql.functions import isnan, isnull


# configs
conf = pyspark.SparkConf().setAll([('spark.app.name', 'guobiao_tsp_tbls.guobiao_vehicle_raw'),
                                   ('spark.master', 'yarn'),
                                   ('spark.submit.deployMode', 'client'),
                                   ('spark.executor.memory', '10g'),
                                   ('spark.memory.fraction', '0.7'),
                                   ('spark.executor.cores', '3'),
                                   ('spark.executor.instances', '20'),
                                   ('spark.yarn.am.memory', '10g'),
                                   ('spark.debug.maxToStringFields','100')])
conf1 = pyspark.SparkConf().setAll([('spark.app.name', 'guobiao_tsp_tbls.guobiao_vehicle_raw'),
                                    ('spark.master', 'local'),
                                    ('spark.executor.memory', '10g'),
                                    ('spark.memory.fraction', '0.7'),
                                    ('spark.executor.cores', '3'),
                                    ('spark.debug.maxToStringFields','100')])

def run_cmd(args_list):
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proc.communicate()
    return proc.returncode


# export to hive
sc = pyspark.SparkContext(conf=conf1)
the_directory = 'hdfs://namenode:8020/data/guobiao/'
logfile ='/home/wchen/dsa/log_guobiao.txt'

dates = []
#start_date = date(2018, 8, 22)
start_date = date.today()
end_date = date(2017, 8, 5)
#end_date = date(2018, 1, 1)
def daterange(start_date, end_date):
    for n in range(int((start_date - end_date).days + 1)):
        yield start_date - timedelta(n)
for single_date in daterange(start_date, end_date):
    dates.append(single_date.strftime('%Y%m%d'))

# Hive context
hc = HiveContext(sc)

a=set()
with open(logfile, 'r') as infile:
    for line in infile:
        if line.strip() not in a:
            a.add(line.strip())
          
for day in dates:
    if day in a:
	break

    file_loc = 'csv/d={}'.format(day)
    data_file = the_directory + file_loc
    returncode = run_cmd(['hdfs', 'dfs', '-test', '-d', data_file])
	#returncode = run_cmd(['hdfs', 'dfs', '-test', '-d', 'hdfs://namenode:8020/data/guobiao/csv/d=20181116'])
    if returncode:
        print('{} does not exist, skipping ..'.format(data_file))
  	continue
	
    filename = the_directory + 'csv/d=' + day + '/'
    sql_cmd = """ALTER TABLE guobiao_tsp_tbls.guobiao_vehicle_raw ADD PARTITION(day='{0}') location'{1}'""".format(day, filename) 

    hc.sql(sql_cmd)
    print(sql_cmd)
    with open(logfile, "a") as myfile:
        myfile.write(day+'\n')

sc.stop()
# print('done.')
