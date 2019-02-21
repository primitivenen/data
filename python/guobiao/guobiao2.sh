#!/bin/bash

cd /home/wchen/dsa/
current_time=$(date "+%Y.%m.%d-%H.%M.%S")
log_file=/home/wchen/dsa/guobiao_vehicle_raw_$current_time.log    
latest_dir=`hdfs dfs -ls -R /data/guobiao/csv/ | grep "^d" | sort -k6,7 | tail -1 | tr -s ' ' | cut -d' ' -f8`
endday=${latest_dir:20:8}
#endday=`date --date="today" +%Y%m%d`
#startday=`date --date="10 days ago" +%Y%m%d`
last_date=`grep \. log_guobiao.txt | tail -1`
startday=$(date -d "$last_date + 1 day" +"%Y%m%d")
python guobiao_data_0815.py   > $log_file
num_err=$(grep 'Traceback (most recent call last)' $log_file -c)
#num_err=$(grep 'Traceback (most recent call last)' /home/wchen/dsa/guobiao_vehicle_raw_2018.11.28-08.23.25.log -c)
if [[ "$num_err" -ge 1 ]]; then
    #echo $log_file
   # mailx -s "Process failed. Check $log_file" wchen@gacrndusa.com < /home/wchen/dsa/guobiao_vehicle_raw_2018.11.28-08.23.25.log
    mailx -s "Process failed. Check $log_file" wchen@gacrndusa.com < ${log_file}
    echo "Process failed2. Check $log_file" >&2
    exit 1
fi
#startday=$(date -d "$last_date" +"%Y%m%d")
d=$startday
echo "Starting populating data for $d"
#:<<EOF
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
#EOF
echo "Process done." 
mailx -s "Process Succeed. Check $log_file" wchen@gacrndusa.com < ${log_file}  
