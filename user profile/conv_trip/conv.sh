cd /home/wchen/dsc
python conv_trip3.py
hive -f get_trip_clean.hql
hive -f get_am_peak_driving.hql
hive -f get_entropy.hql
hive -f get_long_distance.hql
hive -f get_night_driving.hql
hive -f get_pm_peak_driving.hql
hive -f get_combined.hql