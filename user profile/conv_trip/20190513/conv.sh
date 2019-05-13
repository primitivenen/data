cd /home/wchen/dsc
log_file=/home/wchen/dsc/log_conv.txt 
last_date=`grep \. log_conv.txt | tail -1`
startday=$(date -d "$last_date" +"%Y%m%d")
latest_dir=`hdfs dfs -ls -R /apps/hive/warehouse/conv_tsp_tbls.db/a7m_5s_orc | grep "^d" | sort -k6,7 | tail -1 | tr -s ' ' | cut -d' ' -f8`
endday=${latest_dir:52:8}
echo $startday, $endday
d=$startday
while [[ "$d" -le $endday ]]; do
  d=$startday
  d1=$(date --date="$startday+2days" +%Y%m%d)
  python conv_trip3.py $d $d1
  startday=$d1
done
echo $endday >> $log_file
     
