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
	#print (filename)
	if os.path.exists(the_csv_file+'.done'):
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

		colDict = {}
		for name in list(df.columns.values):
			colDict[name] = name.replace('total.battery.energy.throughput.in.kwh', 'total_battery_energy_throughpt_in_kwh').replace('.','_')
		df = df.rename(index=str, columns = colDict)
		
		#df = df[~df.vin.isnan].copy()
		
		#print (colDict)
		if ('BMS.Updated' not in colDict) and ('BMS_Updated' not in colDict):
			print (filename)
			df['BMS_Updated'] = 'NA'
		if ('BMS.Update.Date' not in colDict) and ('BMS_Update_Date' not in colDict):
			print (filename)
			df['BMS_Update_Date'] = 'NA'
		if ('battery.usage.high' not in colDict) and ('battery_usage_high' not in colDict):
			print (filename)
			df['battery_usage_high'] = 100
			
		#df=df[[c for c in df if c not in['BMS_Updated','BMS_Update_Date','battery_usage_high']]+['BMS_Updated','BMS_Update_Date','battery_usage_high']].copy()
		
		#df=df[['VIN','error_reported','num_records','num_days','num_days_active','city_last_day','prov_last_day','start_odo','end_odo','total_distance','total_distance_on_battery','total_distance_on_fuel','daily_dist_avg','daily_dist_max','daily_dist_p99','daily_batt_dist_avg','daily_batt_dist_max','daily_batt_dist_p99','daily_fuel_dist_avg','daily_fuel_dist_max','daily_fuel_dist_p99','total_active_hours','total_charge_hours','total_drive_hours','charge_percent','daily_charge_hours_avg','daily_acc_pos_avg','total_charge_energy_in_kwh','daily_charge_energy_avg_in_kwh','recent_battery_temp_avg_p25','recent_battery_temp_avg_p75','recent_battery_temp_max_p25','recent_battery_temp_max_p75','recent_battery_temp_min_p25','recent_battery_temp_min_p75','recent_battery_SOC_p25','recent_battery_SOC_p75','total_battery_curr_drive_sum','total_battery_curr_drive_abs_sum','total_battery_curr_charge_sum','total_battery_curr_charge_abs_sum','total_battery_curr_sum','total_battery_curr_abs_sum','total_battery_energy_throughpt_in_kwh','total_battery_energy_received_in_kwh','total_battery_energy_consumption_in_kwh','total_energy_consumption_on_battery','total_electricity_energy_efficiency','total_fuel_consumption_in_l','total_fuel_efficiency','life_start_odo','battery_changed','battery_life_in_days','battery_life_in_km','battery_life_in_charge_hours','battery_life_in_drive_hours','battery_life_energy_total_sum','battery_life_energy_discharge_sum','battery_life_energy_discharge_on_battery_sum','battery_life_energy_plugin_charge_sum','battery_life_curr_discharge_in_Ah','battery_life_curr_abs_in_Ah','battery_life_dist_on_battery','battery_life_dist_on_fuel','battery_life_fuel_consumption','battery_usage_high','cell_volt_diff_drive_mean_now','cell_volt_diff_drifted','cell_volt_diff_slope_alerted','cell_volt_diff_pred_alerted','total_alerts_now','odo_index','charge_index','BMS_Updated','BMS_Update_Date','alert_index','normalized_dist_on_battery','seg_battery_drive_energy_capacity_in_kwh','seg_battery_drive_curr_capacity_in_Ah','seg_battery_charge_energy_capacity_in_kwh','seg_battery_charge_curr_capacity_in_Ah','new_seg_battery_drive_energy_capacity_in_kwh','new_seg_battery_drive_curr_capacity_in_Ah','new_seg_battery_charge_energy_capacity_in_kwh','new_normalized_dist_on_battery','new_seg_battery_charge_curr_capacity_in_Ah','curr_capacity_drive_SOH','curr_capacity_charge_SOH','BMS_Updated','BMS_Update_Date','battery_usage_high']].copy()
		df=df[['VIN', 'error_reported', 'num_records', 'start_time', 'end_time', 'num_days', 'num_days_active', 'city_last_day', 'prov_last_day', 'start_odo', 'end_odo', 'total_distance', 'total_distance_on_battery', 'total_distance_on_fuel', 'daily_dist_avg', 'daily_dist_max', 'daily_dist_p99', 'daily_batt_dist_avg', 'daily_batt_dist_max', 'daily_batt_dist_p99', 'daily_fuel_dist_avg', 'daily_fuel_dist_max', 'daily_fuel_dist_p99', 'total_active_hours', 'total_charge_hours', 'total_drive_hours', 'charge_percent', 'daily_charge_hours_avg', 'daily_acc_pos_avg', 'total_charge_energy_in_kwh', 'daily_charge_energy_avg_in_kwh', 'recent_battery_temp_avg_p25', 'recent_battery_temp_avg_p75', 'recent_battery_temp_max_p25', 'recent_battery_temp_max_p75', 'recent_battery_temp_min_p25', 'recent_battery_temp_min_p75', 'recent_battery_SOC_p25', 'recent_battery_SOC_p75', 'total_battery_curr_drive_sum', 'total_battery_curr_drive_abs_sum', 'total_battery_curr_charge_sum', 'total_battery_curr_charge_abs_sum', 'total_battery_curr_sum', 'total_battery_curr_abs_sum', 'total_battery_energy_throughpt_in_kwh', 'total_battery_energy_received_in_kwh', 'total_battery_energy_consumption_in_kwh', 'total_energy_consumption_on_battery', 'total_electricity_energy_efficiency', 'total_fuel_consumption_in_l', 'total_fuel_efficiency', 'life_start_odo', 'battery_changed', 'battery_life_in_days', 'battery_life_in_km', 'battery_life_in_charge_hours', 'battery_life_in_drive_hours', 'battery_life_energy_total_sum', 'battery_life_energy_discharge_sum', 'battery_life_energy_discharge_on_battery_sum', 'battery_life_energy_plugin_charge_sum', 'battery_life_curr_discharge_in_Ah', 'battery_life_curr_abs_in_Ah', 'battery_life_dist_on_battery', 'battery_life_dist_on_fuel', 'battery_changed_date', 'battery_life_fuel_consumption', 'cell_volt_diff_drive_mean_now', 'cell_volt_diff_drifted', 'cell_volt_diff_slope_alerted', 'cell_volt_diff_pred_alerted', 'total_alerts_now', 'odo_index', 'charge_index', 'alert_index', 'normalized_dist_on_battery', 'seg_battery_drive_energy_capacity_in_kwh', 'seg_battery_drive_curr_capacity_in_Ah', 'seg_battery_charge_energy_capacity_in_kwh', 'seg_battery_charge_curr_capacity_in_Ah', 'new_seg_battery_drive_energy_capacity_in_kwh', 'new_seg_battery_drive_curr_capacity_in_Ah', 'new_seg_battery_charge_energy_capacity_in_kwh', 'new_normalized_dist_on_battery', 'new_seg_battery_charge_curr_capacity_in_Ah', 'curr_capacity_drive_SOH', 'curr_capacity_charge_SOH', 'BMS_Updated', 'BMS_Update_Date', 'battery_usage_high']].copy()
		spark_df = hc.createDataFrame([tuple(None if isinstance(x, (long, float)) and np.isnan(x) else x for x in record.tolist()) for record in df.to_records(index=False)], df.columns.tolist())
		#spark_df = hc.createDataFrame(df)
		
		#print len(spark_df.columns)
		cols = [when(~col(x).isin("NULL", "NA", "NaN",""), col(x)).alias(x) for x in spark_df.columns]
		spark_df = spark_df.select(*cols)
				
		spark_df.registerTempTable('update_dataframe')
         
		#print(day)
		#print(d)
		sql_cmd = """INSERT OVERWRITE TABLE tsp_tbls.ag2_vehicle_stats
				  PARTITION (day='{}')
				  SELECT VIN,error_reported,num_records,start_time,end_time,num_days,num_days_active,city_last_day,prov_last_day,start_odo,end_odo,total_distance,total_distance_on_battery,total_distance_on_fuel,daily_dist_avg,daily_dist_max,daily_dist_p99,daily_batt_dist_avg,daily_batt_dist_max,daily_batt_dist_p99,daily_fuel_dist_avg,daily_fuel_dist_max,daily_fuel_dist_p99,total_active_hours,total_charge_hours,total_drive_hours,charge_percent,daily_charge_hours_avg,daily_acc_pos_avg,total_charge_energy_in_kwh,daily_charge_energy_avg_in_kwh,recent_battery_temp_avg_p25,recent_battery_temp_avg_p75,recent_battery_temp_max_p25,recent_battery_temp_max_p75,recent_battery_temp_min_p25,recent_battery_temp_min_p75,recent_battery_SOC_p25,recent_battery_SOC_p75,total_battery_curr_drive_sum,total_battery_curr_drive_abs_sum,total_battery_curr_charge_sum,total_battery_curr_charge_abs_sum,total_battery_curr_sum,total_battery_curr_abs_sum,total_battery_energy_throughpt_in_kwh,total_battery_energy_received_in_kwh,total_battery_energy_consumption_in_kwh,total_energy_consumption_on_battery,total_electricity_energy_efficiency,total_fuel_consumption_in_l,total_fuel_efficiency,life_start_odo,battery_changed,battery_life_in_days,battery_life_in_km,battery_life_in_charge_hours,battery_life_in_drive_hours,battery_life_energy_total_sum,battery_life_energy_discharge_sum,battery_life_energy_discharge_on_battery_sum,battery_life_energy_plugin_charge_sum,battery_life_curr_discharge_in_Ah,battery_life_curr_abs_in_Ah,battery_life_dist_on_battery,battery_life_dist_on_fuel,battery_changed_date,battery_life_fuel_consumption,cell_volt_diff_drive_mean_now,cell_volt_diff_drifted,cell_volt_diff_slope_alerted,cell_volt_diff_pred_alerted,total_alerts_now,odo_index,charge_index,alert_index,normalized_dist_on_battery,seg_battery_drive_energy_capacity_in_kwh,seg_battery_drive_curr_capacity_in_Ah,seg_battery_charge_energy_capacity_in_kwh,seg_battery_charge_curr_capacity_in_Ah,new_seg_battery_drive_energy_capacity_in_kwh,new_seg_battery_drive_curr_capacity_in_Ah,new_seg_battery_charge_energy_capacity_in_kwh,new_normalized_dist_on_battery,new_seg_battery_charge_curr_capacity_in_Ah,curr_capacity_drive_SOH,curr_capacity_charge_SOH,BMS_Updated,BMS_Update_Date,battery_usage_high FROM update_dataframe""".format(day)
		#print(sql_cmd)
		hc.sql(sql_cmd)

		sql_cmd = """SELECT COUNT(*) FROM tsp_tbls.ag2_vehicle_stats
				  WHERE day='{}' """.format(day)
		#print(sql_cmd)

        with open(work_file+'.done', 'w') as f:
			pass

sc.stop()
print('done.')

