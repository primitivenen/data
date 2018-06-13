from datetime import datetime, date
import subprocess
import pandas as pd
import os
import sys
import findspark   # find spark home directory
findspark.init("/usr/hdp/current/spark2-client")   # spark location on namenode server

import numpy as np
import pyspark
from pyspark.sql import HiveContext
from pyspark.sql.functions import col, when

from pyspark.sql.functions import isnan, isnull


# configs
conf = pyspark.SparkConf().setAll([('spark.app.name', 'tsp_tbls.ag2_vehicle_stats'),
                                   ('spark.master', 'yarn'),
                                   ('spark.submit.deployMode', 'client'),
                                   ('spark.executor.memory', '10g'),
                                   ('spark.memory.fraction', '0.7'),
                                   ('spark.executor.cores', '3'),
                                   ('spark.executor.instances', '20'),
                                   ('spark.yarn.am.memory', '10g'),
                                   ('spark.debug.maxToStringFields','100')])
conf1 = pyspark.SparkConf().setAll([('spark.app.name', 'tsp_tbls.ag2_vehicle_stats'),
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
the_directory = '/home/elk-daily-updates/StatFinalFiles/'

# Hive context
hc = HiveContext(sc)

columnsToDelete = ['file.names', 'plotted']

files=sorted([f for f in os.listdir(the_directory) if '.done' not in f])

for filename in files:
	the_csv_file = os.path.join(the_directory, filename)
	print (filename)
	if os.path.exists(filename+'.done'):
		print ('{} uploaded already, skipping'.format(f))
		continue
	
	if os.path.isfile(the_csv_file):
		d = filename.split('_')[1].split('.')[0]
		df = pd.read_csv(the_csv_file)
		#print (d)
		df.drop(columnsToDelete, axis=1, inplace=True)
		
		#print (df.columns.values)
		
		day = pd.to_datetime(str(d)).date().strftime('%Y-%m-%d')  # convert date format
		
		work_file = the_directory+'StatFinal_{}.csv'.format(d)
		
		#df = df.withColumn("updateday", lit(d))
		colDict = {}
		for name in list(df.columns.values):
			colDict[name] = name.replace('.','_')
		df = df.rename(index=str, columns = colDict)
		if 'battery.usage.high' not in colDict:
			df['battery_usage_high'] = 0
		if 'BMS.Updated' not in colDict:
			df['BMS_Updated'] = ''
		if 'BMS.Update.Date' not in colDict:
			df['BMS_Update_Date'] = ''
		#df["updateday"] = d
		#df['day'] = day
		#df.drop(labels='day', axis=1, inplace=True)
		#del df['day']
		#print (df.columns.values)
		#print len(df.columns.values)
   
		spark_df = hc.createDataFrame([tuple(None if isinstance(x, (float, int)) and np.isnan(x) else x for x in record.tolist()) for record in df.to_records(index=False)], df.columns.tolist())
		
		print len(spark_df.columns)
		# convert NaN to null, as they are different in Hive
		#cols = [when(~col(x).isin("NULL", "NA", "NaN", ""), col(x)).alias(x) for x in spark_df.columns]
		#cols = [when(~isnan(x), col(x)).alias(x) if t in ("double", "float") else x for x, t in spark_df.dtypes]

		#cols = [when(~col(x).isin(''), col(x)).alias(x) for x in spark_df.columns]
		
		#print (cols)
		
		#spark_df = spark_df.select(*cols)
		spark_df.registerTempTable('update_dataframe')
		#print (cols)		

		#sql_cmd = """SELECT COUNT(*) FROM tsp_tbls.ag2_vehicle_stats
		#		  WHERE day='{}' """.format(day)
		#print(sql_cmd)
		#hc.sql(sql_cmd).show()
         
		print(day)
		print(d)
		sql_cmd = """INSERT OVERWRITE TABLE tsp_tbls.ag2_vehicle_stats
				  PARTITION (day='{}')
				  SELECT * FROM update_dataframe""".format(day)
		print(sql_cmd)
		hc.sql(sql_cmd)

		sql_cmd = """SELECT COUNT(*) FROM tsp_tbls.ag2_vehicle_stats
				  WHERE day='{}' """.format(day)
		print(sql_cmd)
		#hc.sql(sql_cmd).show()

# ts = pd.to_datetime(datetime.now())
# print('{}: {} has been exported to Hive (overwrite mode)'.format(ts, stats_file))
# write a marker file
        with open(work_file+'.done', 'w') as f:
			pass

sc.stop()
print('done.')

