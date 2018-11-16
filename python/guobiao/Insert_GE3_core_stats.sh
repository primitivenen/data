cd /home/wchen/dsa/

endday=`date --date="today" +%Y-%m-%d`
startday=`date --date="1 day ago" +%Y-%m-%d`

d=$startday
#returncode=`hdfs dfs -test -d hdfs://namenode:8020/data/guobiao/csv/d=$d`
hdfs dfs -test -d hdfs://namenode:8020/data/guobiao/csv/d=$d
returncode=$?
echo $returncode
if [[ "$returncode" -eq 1 ]]; then
	echo "No new data for $d"
else
	f=insert_ge3_$d.hql
	echo "USE guobiao_tsp_tbls; " >> $f
	echo "INSERT OVERWRITE TABLE ge3_core_stats " >> $f
	echo "PARTITION (day=\"$d\") " >> $f
	echo "select b.vin as vin, cast(a.max_volt as decimal(10,3)) as max_volt, cast(a.min_volt as decimal(10,3)) as min_volt, a.max_insulation, a.min_insulation, cast(a.avg_insulation as decimal(10,3)) as avg_insulation, cast(b.battery_temp_max_charging as decimal(10,1)) as battery_temp_max_charging, cast(b.battery_temp_max_driving as decimal(10,1)) as battery_temp_max_driving, cast(b.battery_temp_mean_charging as decimal(10,1)) as battery_temp_mean_charging, cast(b.battery_temp_mean_driving as decimal(10,1)) as battery_temp_mean_driving,cast(b.battery_temp_min_charging as decimal(10,1)) as battery_temp_min_charging, cast(b.battery_temp_min_driving as decimal(10,1)) as battery_temp_min_driving, cast(b.cell_volt_diff_mean_charging as decimal(10,4)) as cell_volt_diff_mean_charging, cast(b.cell_volt_diff_mean_driving as decimal(10,4)) as cell_volt_diff_mean_driving, cast(b.normalized_ah_charged_mean as decimal(10,1)) as normalized_ah_charged_mean, cast(b.normalized_ah_consumption_mean as decimal(10,1)) as normalized_ah_consumption_mean, cast(b.normalized_distance_driven_mean as decimal(10,1)) as normalized_distance_driven_mean, CASE WHEN max_volt >= 4.202 or avg_insulation < 990 or battery_temp_max_charging > 40 or battery_temp_max_driving > 40 or cell_volt_diff_mean_charging > 0.04 or cell_volt_diff_mean_driving > 0.04 THEN 1 ELSE 0 END AS high_risk from ge3_filter_result a full outer join vin_ranking b on a.vin=b.vin where b.end_date in (select CASE WHEN max(end_date)=\"$d\" THEN max(end_date) ELSE NULL END from vin_ranking) " >> $f
	echo "Executing... hive -f $f;"
	hive -f $f
	echo "Done with hive -f $f" >> ge3_insert_${startday}_${endday}.log 
	rm /home/wchen/dsa/$f
fi
