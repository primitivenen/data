#!/bin/bash
cd /home/wchen/dsa/
python guobiao_data_0815.py   |& tee -a /home/wchen/dsa/loglog 
endday=`date --date="today" +%Y%m%d`
#startday=`date --date="10 days ago" +%Y%m%d`
last_date=`grep \. log_guobiao.txt | tail -1`
startday=$(date -d "$last_date + 1 day" +"%Y%m%d")
d=$startday
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
  echo "Done with hive -f $f" >> batch_insert_${startday}_${endday}.log
  d=$(date --date="$d + 1 day" +%Y%m%d)
  rm /home/wchen/dsa/$f
done
current_time=$(date "+%Y.%m.%d-%H.%M.%S")
hive -f get_high_cell_volt_diff_all_records.hql  > /home/wchen/dsa/high_cell_volt_diff_all_records_$current_time.log 2>&1
hive -f get_high_cell_volt_diff_by_vin.hql  > /home/wchen/dsa/high_cell_volt_diff_by_vin_$current_time.log 2>&1
hive -f guobiao_filter_all.hql  > /home/wchen/dsa/filter_all_$current_time.log  2>&1
hive -f guobiao_filter_vin.hql  > /home/wchen/dsa/filter_vin_$current_time.log   2>&1
bash Insert_GE3_core_stats.sh  > /home/wchen/dsa/ge3_core_stats_$current_time.log   2>&1
