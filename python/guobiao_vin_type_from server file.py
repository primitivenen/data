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

conf = pyspark.SparkConf().setAll([('spark.app.name', 'guobiao_tsp_tbls.guobiao_vin_type'),
                                   ('spark.master', 'yarn'),
                                   ('spark.submit.deployMode', 'client'),
                                   ('spark.executor.memory', '10g'),
                                   ('spark.memory.fraction', '0.7'),
                                   ('spark.executor.cores', '3'),
                                   ('spark.executor.instances', '20'),
                                   ('spark.yarn.am.memory', '10g'),
                                   ('spark.debug.maxToStringFields','100')])
conf1 = pyspark.SparkConf().setAll([('spark.app.name', 'guobiao_tsp_tbls.guobiao_vin_type'),
                                    ('spark.master', 'local'),
                                    ('spark.executor.memory', '10g'),
                                    ('spark.memory.fraction', '0.7'),
                                    ('spark.executor.cores', '3'),
                                    ('spark.debug.maxToStringFields','100')])

def run_cmd(args_list):
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proc.communicate()
    return proc.returncode

sc = pyspark.SparkContext(conf=conf1)
the_file = '/home/wchen/dsa/...csv'

hc = HiveContext(sc)

if os.path.isfile(the_file):
		df = pd.read_csv(the_file,names=["vin","vintype"])

		spark_df = hc.createDataFrame([tuple(None if isinstance(x, (long, float)) and np.isnan(x) else x for x in record.tolist()) for record in df.to_records(index=False)], df.columns.tolist())

		cols = [when(~col(x).isin("NULL", "NA", "NaN",""), col(x)).alias(x) for x in spark_df.columns]
		spark_df = spark_df.select(*cols)
				
		spark_df.registerTempTable('update_dataframe')

		sql_cmd = """INSERT OVERWRITE TABLE guobiao_tsp_tbls.guobiao_vin_type
				  SELECT * FROM update_dataframe"""

		hc.sql(sql_cmd)

sc.stop()
print('done.')