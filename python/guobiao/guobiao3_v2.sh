cd /home/wchen/dsa/
echo "Creating tables ..."

log_file=/home/wchen/dsa/log_guobiao2.txt  
last_date=`grep \. log_guobiao2.txt | sort | tail -1`
latest_dir=`hdfs dfs -ls -R /data/guobiao/csv/ | grep "^d" | sort -k6,7 | tail -1 | tr -s ' ' | cut -d' ' -f8`

if [[ -z $1 ]]
then
    #startday=$(date -d "$last_date + 1 day" +"%Y%m%d")
    startday=$(date -d "$last_date" +"%Y%m%d")
else
    startday=$1
fi

if [[ -z $2 ]]
then
    endday=${latest_dir:20:8}
else
    endday=$2
fi

if [ "$endday" -lt "$startday" ]; then
    endday="$startday"
fi

#startday=$(date -d "$last_date" +"%Y%m%d")

d=$startday
while [[ "$d" -le $endday ]]; do
  echo hdfs://namenode:8020/data/guobiao/csv/d=$d
  hdfs dfs -test -e hdfs://namenode:8020/data/guobiao/csv/d=$d/_SUCCESS
  returncode=$?
  echo $returncode
  if [[ "$returncode" -eq 1 ]]; then
    echo "No new data for $d"
    echo "$returncode"
  else
    tmp=`date -d $d +"%Y-%m-%d"`
    sed -i -e "s/current_date/'$tmp'/g" get_high_cell_volt_diff_all_records.hql
    hive -f get_high_cell_volt_diff_all_records.hql  #> /home/wchen/dsa/high_cell_volt_diff_all_records_$current_time.log 
    sed -i -e "s/'$tmp'/current_date/g" get_high_cell_volt_diff_all_records.hql
    hive -f get_high_cell_volt_diff_by_vin.hql  #> /home/wchen/dsa/high_cell_volt_diff_by_vin_$current_time.log 
    sed -i -e "s/current_date/'$tmp'/g" guobiao_filter_all.hql    
    hive -f guobiao_filter_all.hql  #> /home/wchen/dsa/filter_all_$current_time.log 
    sed -i -e "s/'$tmp'/current_date/g" guobiao_filter_all.hql   
    hive -f guobiao_filter_vin.hql  #> /home/wchen/dsa/filter_vin_$current_time.log 
    sed -i -e "s/startday/$tmp/g" Insert_GE3_core_stats.sh  
    echo "/startday/$tmp/"
    bash Insert_GE3_core_stats.sh  #> /home/wchen/dsa/ge3_core_stats_$current_time.log   
    sed -i -e "s/$tmp/startday/g" Insert_GE3_core_stats.sh  
    echo "/$tmp/startday/"
  fi
  d=$(date --date="$d + 1 day" +%Y%m%d)
  echo $d >> $log_file
done

echo "Process done." 
