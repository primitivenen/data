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
#hive -f get_trip_clean.hql
#echo "trip created,starting generating tables..."
#startday=$(date -d "$last_date" +"%Y%m%d")
#echo $startday, $endday
#d=$startday
#while [[ "$d" -le $end_date ]]; do
  #tmp=`date -d $d +"%Y-%m-%d"`
  #echo $tmp
  #sed -i -e "s/current_date/'$tmp'/g" get_am_peak_driving.hql
  #hive -f get_am_peak_driving.hql
  #sed -i -e "s/'$tmp'/current_date/g" get_am_peak_driving.hql
 # hive -f get_entropy.hql
  #sed -i -e "s/current_date/'$tmp'/g" get_long_distance.hql
  #hive -f get_long_distance.hql
  #sed -i -e "s/'$tmp'/current_date/g" get_long_distance.hql
  #sed -i -e "s/current_date/'$tmp'/g" get_night_driving.h ql
  #hive -f get_night_driving.hql
  #sed -i -e "s/'$tmp'/current_date/g" get_night_driving.hql
  #sed -i -e "s/current_date/'$tmp'/g" get_pm_peak_driving.hql
  #hive -f get_pm_peak_driving.hql
  #sed -i -e "s/'$tmp'/current_date/g" get_pm_peak_driving.hql
#  d=$(date --date="$d+1 day" +%Y%m%d)
#done
#hive -f get_am_peak_driving_all.hql
#hive -f get_long_distance_all.hql
#hive -f get_night_driving_all.hql
#hive -f get_pm_peak_driving_all.hql
#hive -f get_combined_all.hql
echo $endday >> $log_file
     
