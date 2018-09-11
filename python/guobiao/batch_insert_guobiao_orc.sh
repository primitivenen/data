cd /home/wchen/dsa/

endday=`date --date="today" +%Y%m%d`
startday=`date --date="10 days ago" +%Y%m%d`

d=$startday
while [[ "$d" -le $endday ]]; do
  f=insert_$d.hql
  echo "USE guobiao_tsp_tbls;" > $f
  echo "INSERT OVERWRITE TABLE guobiao_raw_orc" >> $f
  echo "PARTITION (day=\"$d\")" >> $f
  echo "SELECT \`(day)?+.+\`" >> $f
  echo "FROM guobiao_vehicle_raw" >> $f
  echo "WHERE day=\"$d\"" >> $f
  echo "Executing... hive -f $f"
  hive -f $f
  echo "Done with hive -f $f" >> batch_insert_${startday}_${endday}.log
  d=$(date --date="$d + 1 day" +%Y%m%d)
done


