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
logfile ='/home/wchen/dsa/log2.txt'

dates = []
start_date = date(2018, 7, 2)
#start_date = date.today()
end_date = date(2017, 5, 17)
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
        if line not in a:
            a.add(line)
            
b=set()
sdate = date(2018,2,12)
edate = date(2017,5,17)

for single_date in daterange(sdate, edate):
    b.add(single_date.strftime('%Y%m%d'))
        
for day in dates:
	if day in a:
		break
	if day in b:
	    file_loc = 'csv/{}_new.csv'.format(day)
	    data_file = the_directory + file_loc
   	    returncode = run_cmd(['hdfs', 'dfs', '-test', '-e', data_file])
   	    if returncode:
  		print('{} does not exist, skipping ..'.format(data_file))
  		continue
	else:
	    file_loc = 'csv/d={}'.format(day)
	    data_file = the_directory + file_loc
   	    returncode = run_cmd(['hdfs', 'dfs', '-test', '-d', data_file])
   	    if returncode:
  		print('{} does not exist, skipping ..'.format(data_file))
  		continue


	if day in b:
	   filename = the_directory + 'csv/'
	   sql_cmd = """ALTER TABLE guobiao_tsp_tbls.guobiao_vehicle_raw ADD PARTITION(day='{0}') location'{1}'""".format(day, filename) 
	   #print (sql_cmd)
	else:
	   filename = the_directory + 'csv/d=' + day + '/'
	   sql_cmd = """ALTER TABLE guobiao_tsp_tbls.guobiao_vehicle_raw ADD PARTITION(day='{0}') location'{1}'""".format(day, filename) 
	   #print (sql_cmd)
	hc.sql(sql_cmd)
	print(sql_cmd)
	with open(logfile, "a") as myfile:
            myfile.write(day+'\n')

sc.stop()
# print('done.')
