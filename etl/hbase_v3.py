
```
"""
@date 2021/3/10
@author: Linda.Chen

Updating sum table incrementally using HBase
(Testing Stage)
================
"""
from typing import *
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, HiveContext
from recommender.common.util import json_util, sparksession_util as ss_util
from recommender.common.util.hbase_util import HbaseConnector
import time


class UserData:
    """
        This class provides PySpark functions for data input.
    """

    def __init__(self, sql_context: SparkSession, hive_table: str = 'onzoom_user_activity_data', db: str = 'dw',
                 partition: str = "2021-02-24-01"):
        self.sql_context = sql_context
        self.hive_table = hive_table
        self.db = hive_db
        self.partition = partition

    def get_onzoom_user_activity_data(self) -> List[Dict[str, str]]:
        sql = """                                                                                                                              
                select                                                                                                                         
                    t.*,                                                                                                                       
                    case                                                                                                                       
                        when t.like_event_time is NULL and t.unlike_event_time is NULL then NULL                                               
                        when t.like_event_time is NULL and t.unlike_event_time is NOT NULL then false                                          
                        when t.like_event_time is NOT NULL and t.unlike_event_time is NULL then true                                           
                        when t.like_event_time > t.unlike_event_time then true                                                                 
                        else false                                                                                                             
                    end as is_liked                                                                                                            
                from(                                                                                                                          
                    select                                                                                                                     
                        user_id,                                                                                                               
                        event_id,                                                                                                              
                        '' as annotation,                                                                                                      
                        sum(if (action_type == "click", action_val, 0)) as click,                                                              
                        sum(if (action_type == "gift", action_val, 0)) as gift,                                                                
                        sum(if (action_type == "like_event", action_val, 0)) as like_event,                                                    
                        sum(if (action_type == "order", action_val, 0)) as order_event,                                                        
                        sum(if (action_type == "unlike_event", action_val, 0)) as unlike_event,                                                
                        sum(if (action_type == "rating", action_val, 0)) as rating,                                                            
                        max(log_time) as log_time,                                                                                             
                        max(case when action_type = "like_event" then log_time end) as like_event_time,                                        
                        max(case when action_type = "unlike_event" then log_time end) as unlike_event_time                                     
                    from {}.{}                                                                                                                 
                    where user_id is not null                                                                                                  
                        and event_id is not null                                                                                               
                        and create_date = '{}'                                                                                                          
                    group by                                                                                                                   
                        user_id,                                                                                                               
                        event_id,                                                                                                              
                        annotation) t                                                                                                          
               """.format(
            self.hive_db,
            self.hive_table,
            self.partition)

        activity_data = [{"user_id": x["user_id"], "event_id": x["event_id"],
                          "annotation": x["annotation"], "click": x["click"],
                          "gift": x["gift"], "like_event": x["like_event"],
                          "order_event": x["order_event"], "unlike_event": x["unlike_event"],
                          "rating": x["rating"], "log_time": x["log_time"], "is_liked": x["is_liked"]}
                         for x in self.sql_context.sql(sql).rdd.collect()]

        return activity_data


if __name__ == "__main__":
    """
    current_time: "2021-02-24-01"
    """
    on_dev = False
    current_str_time = None
    if len(sys.argv) == 2:
        if sys.argv[1] == "dev":
            on_dev = True
        else:
            current_str_time = sys.argv[1]
    else:
        current_str_time = sys.argv[1]
        on_dev = True

    spark = ss_util.get_spark_session(app_name="onzoom_user_activity_sum_generator_Linda",
                                      configs={"spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                                               "spark.sql.hive.convertMetastoreParquet": "false"},
                                      enable_hive=True)

    logger = ss_util.get_logger(spark, "onzoom_user_activity_sum")

    config_file = "./recommender.json"
    config = json_util.load_json(config_file)
    config_etl = config.get("etl")
    hive_db = config_etl.get("hive_db")
    hive_onzoom_user_activity_data = config_etl.get("hive_onzoom_user_activity_data")
    hbase_onzoom_user_activity_sum = config_etl.get("hbase_onzoom_user_activity_sum")
    hbase_onzoom_user_updated = config_etl.get("hbase_onzoom_user_updated")
    hb_port = config_etl.get("hbase_port")
    hb_host = config_etl.get("hbase_host")
    schedule_time_delta = int(config_etl.get("schedule_time_delta"))

    end_time = datetime.now() if not current_str_time else datetime.strptime(current_str_time, '%Y-%m-%d-%H')
    start_time = end_time - timedelta(hours=schedule_time_delta)
    start_str_time = start_time.strftime("%Y-%m-%d-%H")

    data_extractor = UserData(spark, hive_onzoom_user_activity_data, hive_db, start_str_time)
    hive_data = data_extractor.get_onzoom_user_activity_data()

    hbase_con = HbaseConnector(hb_port, hb_host)
    table = hbase_con.get_table(hbase_onzoom_user_activity_sum)
    user_table = hbase_con.get_table(hbase_onzoom_user_updated)

    columns = {'user_id': 'uid', 'event_id': 'eid', 'annotation': 'ant', 'click': 'click',
               'gift': 'gift', 'like_event': 'like', 'order_event': 'order', 'unlike_event': 'unlike',
               'rating': 'rating', 'log_time': 'lgt', 'is_liked': 'isliked'}

    cf_name = 'cf'
    b = table.batch()
    user_b = user_table.batch()
    start = time.time()
    counter = 0
    for row in hive_data:
        counter += 1
        cur_id = row["user_id"] + '_' + row["event_id"]
        refer_id = row["log_time"].strftime("%Y%m%d%H%M%S") + '_' + row["user_id"]
        user_b.put(refer_id, {hbase_con.encode_utf8("cf:uid"): hbase_con.encode_utf8(row["user_id"])})
        user_b.put(refer_id, {hbase_con.encode_utf8("cf:eid"): hbase_con.encode_utf8(row["event_id"])})
        old_data = table.row(cur_id)
        for col, hb_col in columns.items():
            hb_cf_str = cf_name + ':' + hb_col
            hb_cf = hbase_con.encode_utf8(hb_cf_str)
            val = row[col]
            if col == 'is_liked' and not row[col]:
                continue
            if old_data is not None and old_data and hb_cf in old_data \
                    and col != 'annotation' and col != 'log_time' and col != 'is_liked' \
                    and col != 'event_id' and col != 'user_id':
                if float(val) == 0.0:
                    continue
                val = float(val) + float(old_data[hb_cf])
            b.put(cur_id, {hb_cf: hbase_con.encode_utf8(str(val))})
        if counter % 2500 == 0:
            b.send()
    user_b.send()
    b.send()
    end = time.time()
    logger.info("Elapsed time is " + str(end - start))

```
