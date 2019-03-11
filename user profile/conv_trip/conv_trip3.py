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
        ('spark.executor.instances', '10'),    # number of executors in total
        ('spark.yarn.am.memory', '10g')])      # memory for spark driver (application master)
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
   
    
if __name__ == '__main__':
    main()    
