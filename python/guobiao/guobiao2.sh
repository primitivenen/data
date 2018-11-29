#!/bin/bash

cd /home/wchen/dsa/
current_time=$(date "+%Y.%m.%d-%H.%M.%S")
log_file=/home/wchen/dsa/guobiao_vehicle_raw_$current_time.log    
python guobiao_data_0815.py   > $log_file
num_err=$(grep 'Traceback (most recent call last)' $log_file -c)
#num_err=$(grep 'Traceback (most recent call last)' /home/wchen/dsa/guobiao_vehicle_raw_2018.11.28-08.23.25.log -c)
if [ "$num_err" -ge 1 ]; then
    #echo $log_file
   # mailx -s "Process failed. Check $log_file" wchen@gacrndusa.com < /home/wchen/dsa/guobiao_vehicle_raw_2018.11.28-08.23.25.log
    mailx -s "Process failed. Check $log_file" wchen@gacrndusa.com < ${log_file}
    echo "Process failed2. Check $log_file" >&2
    exit 1
fi
endday=`date --date="today" +%Y%m%d`
#startday=`date --date="10 days ago" +%Y%m%d`
last_date=`grep \. log_guobiao.txt | tail -1`
#startday=$(date -d "$last_date + 1 day" +"%Y%m%d")
startday=$(date -d "$last_date" +"%Y%m%d")
d=$startday
echo "Starting populating data for $d"
while [[ "$d" -le $endday ]]; do
  #returncode=`hdfs dfs -test -d hdfs://namenode:8020/data/guobiao/csv/d=$d`
  hdfs dfs -test -d hdfs://namenode:8020/data/guobiao/csv/d=$d
  returncode=$?
  echo "For date $d, returncode is $returncode"
  if [[ "$returncode" -eq 1 ]]; then
    d=$(date --date="$d + 1 day" +%Y%m%d)
    continue
  fi
  f=insert_$d.hql
  echo "USE guobiao_tsp_tbls; " >> $f
  echo "INSERT OVERWRITE TABLE guobiao_raw_orc " >> $f
  echo "PARTITION (day=\"$d\") " >> $f
  echo "SELECT \`(day)?+.+\` " >> $f
  echo " FROM guobiao_vehicle_raw " >> $f
  echo "WHERE day=\"$d\" " >> $f
  echo "Executing... hive -f $f;"
  hive -f $f
  #echo "Done with hive -f $f" >> batch_insert_${startday}_${endday}.log
  echo "Done with populating $startday"
  d=$(date --date="$d + 1 day" +%Y%m%d)
  rm /home/wchen/dsa/$f
done
hdfs dfs -test -d hdfs://namenode:8020/data/guobiao/csv/d=$endday
echo hdfs://namenode:8020/data/guobiao/csv/d=$endday
returncode=$?
echo $returncode
if [[ "$returncode" -eq 1 ]]; then
	echo "No new data for $endday"
  echo "$returncode"
else
  echo "Creating tables ..."
  d=$startday
  while [[ "$d" -le $endday ]]; do
    tmp=`date -d $d +"%Y-%m-%d"`
    sed -i -e "s/current_date/'$tmp'/g" get_high_cell_volt_diff_all_records.hql
    hive -f get_high_cell_volt_diff_all_records.hql  > /home/wchen/dsa/high_cell_volt_diff_all_records_$current_time.log 
    sed -i -e "s/'$tmp'/current_date/g" get_high_cell_volt_diff_all_records.hql
    hive -f get_high_cell_volt_diff_by_vin.hql  > /home/wchen/dsa/high_cell_volt_diff_by_vin_$current_time.log 
    sed -i -e "s/current_date/'$tmp'/g" guobiao_filter_all.hql    
    hive -f guobiao_filter_all.hql  > /home/wchen/dsa/filter_all_$current_time.log 
    sed -i -e "s/'$tmp'/current_date/g" guobiao_filter_all.hql   
    hive -f guobiao_filter_vin.hql  > /home/wchen/dsa/filter_vin_$current_time.log   
    bash Insert_GE3_core_stats.sh  > /home/wchen/dsa/ge3_core_stats_$current_time.log   
    d=$(date --date="$d + 1 day" +%Y%m%d)
  done
  echo "Process done." 
  mailx -s "Process Succeed. Check $log_file" wchen@gacrndusa.com < ${log_file}  
fi