#!/bin/bash
cd /home/wchen/dsa/
current_time=$(date "+%Y.%m.%d-%H.%M.%S")
log_file=/home/wchen/dsa/ag_vehicle_raw_$current_time.log   
python ag_data.py > $log_file 
#python ag_data.py |& tee -a /home/wchen/dsa/ag_log 
num_err=$(grep 'Traceback (most recent call last)' $log_file -c)
if [ "$num_err" -ge 1 ]; then
    mailx -s "Process failed. Check $log_file" wchen@gacrndusa.com < ${log_file}
    echo "Process failed. Check $log_file" >&2
    exit 1
fi
dir=`hdfs dfs -ls -R /data/ag/csv/ | grep "^d" | sort -k6,7 | tail -1 | tr -s ' ' | cut -d' ' -f8`
endday=${dir:15:8}
#endday=`date --date="today" +%Y%m%d`
last_date=`grep \. log.txt | tail -1`
startday=$(date -d "$last_date" +"%Y%m%d")
d=$startday
echo "Starting populating data for $d"
while [ "$d" -le "$endday" ]; do
  hdfs dfs -test -d hdfs://namenode:8020/data/ag/csv/d=$d
  returncode=$?
  echo "For date $d, returncode is $returncode"
  if [[ "$returncode" -eq 1 ]]; then
    d=$(date --date="$d + 1 day" +%Y%m%d)
    continue
  fi
  f=insert_$d.hql
  echo "USE tsp_tbls;" >> $f
  echo "INSERT OVERWRITE TABLE ag2_raw_orc" >> $f
  echo "PARTITION (sdate=\"$d\")" >> $f
  echo "SELECT \`(sdate)?+.+\`" >> $f
  echo "FROM ag2_vehicle_raw" >> $f
  echo "WHERE sdate=\"$d\"" >> $f
  echo "Executing... hive -f $f"
  hive -f $f
  echo "Done with hive -f $f" >> batch_insert_ag_${startday}_${endday}.log
  d=$(date --date="$d + 1 day" +%Y%m%d)
  rm /home/wchen/dsa/$f
done
echo "Process done." 
mailx -s "Process Succeed. Check $log_file" wchen@gacrndusa.com < ${log_file}  

