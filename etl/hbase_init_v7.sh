
#!/bin/bash

###################################
#Description   : init hbase tables
#Author        : xxxx
###################################

echo 'list_namespace' | hbase shell 2>/dev/null | grep recommender
is_exist=$?
if [[ "$is_exist" -eq 0 ]]; then
        echo "Hbase namespace already exist."
else
        echo "create_namespace 'recommender'" | hbase shell -n
        status_code=$?
        if [ $status_code -ne 0 ]; then
        echo "The hbase create namespace command may have failed."
        fi
fi

f=create_hbase_table.hql
hive_sum_table="hbase_xxxx_user_activity_sum"
hbase_sum_table="recommender:xxxx_user_activity_sum"
hive_update_table="hbase_xxxx_user_updated"
hbase_update_table="recommender:xxxx_user_updated"
echo "CREATE TABLE IF NOT EXISTS $hive_sum_table" >> $f
echo "(id string,user_id string,event_id string,click int,gift int,like_event int,order_event int,unlike_event int,rating float,log_time bigint,annotation string,is_liked boolean)" >> $f
echo "STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'" >> $f
echo "WITH SERDEPROPERTIES (\"hbase.columns.mapping\" = \":key,cf:uid,cf:eid,cf:click,cf:gift,cf:like,cf:order,cf:unlike,cf:rating,cf:lgt,cf:ant,cf:isliked\")" >> $f
echo "TBLPROPERTIES (\"hbase.table.name\" = \"$hbase_sum_table\");" >> $f
echo "CREATE TABLE IF NOT EXISTS $hive_update_table" >> $f
echo "(id string,user_id string,event_id string)" >> $f
echo "STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'" >> $f
echo "WITH SERDEPROPERTIES (\"hbase.columns.mapping\" = \":key,cf:uid,cf:eid\")" >> $f
echo "TBLPROPERTIES (\"hbase.table.name\" = \"$hbase_update_table\");" >> $f
hive -f $f
echo "Done with hive -f $f"
rm $f

return_error_code=$?
exit $return_error_code

