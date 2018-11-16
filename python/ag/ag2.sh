#!/bin/bash
cd /home/wchen/dsa/
python ag_data.py |& tee -a /home/wchen/dsa/ag_log 
endday=`date --date="today" +%Y%m%d`
startday=`date --date="$days days ago" +%Y%m%d`
d=$startday
while [ "$d" -le "$endday" ]; do
  returncode=`hdfs dfs -test -d hdfs://namenode:8020/data/ag/csv/d=$d`
  if [[ "$returncode" -eq 1 ]]; then
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


