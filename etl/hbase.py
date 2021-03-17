"""
@date 2021/3/10
@author: XXX

Updating sum table incrementally using HBase
(Testing Stage)
================
"""

from pyspark.sql import SparkSession, HiveContext
import happybase
import time

# Spark配置在下一版本中会采用util中的架构。目前的设计为了便于测试而减少了对于其他模块的dependency
spark = SparkSession \
    .builder \
    .appName("LindaTest") \
    .enableHiveSupport() \
    .getOrCreate()

sc = spark.sparkContext
sqlContext = HiveContext(sc)

sql = """
           select
            t.*,
           case 
           when like_event_time is NULL and unlike_event_time is NULL then NULL
           when like_event_time is NULL and  unlike_event_time is NOT NULL then false
           when like_event_time is NOT NULL and unlike_event_time is NULL then true
           when like_event_time > unlike_event_time then true
           else false
           end as is_liked
           from
           (select
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
            and year='{}'
            and month='{}'
            and day='{}'
            and hour='{}'
        group by 
            user_id,
            event_id,
            annotation) t""".format(
    "recommender",
    "onzoom_user_activity_data",
    "2021",
    "02",
    "18",
    "01")

tupleList = [{"user_id": x["user_id"], "event_id": x["event_id"], "annotation": x["annotation"],
              "click": x["click"], "gift": x["gift"], "like_event": x["like_event"], "order_event": x["order_event"],
              "unlike_event": x["unlike_event"], "rating": x["rating"], "log_time": x["log_time"], "is_liked":x["is_liked"]}

             for x in sqlContext.sql(sql).rdd.collect()]

hbase_con = happybase.Connection(host='ip-10-0-15-27.ec2.internal', port=9090)
print(hbase_con.tables())

table = hbase_con.table('recommender:onzoom_user_activity_sum')
user_table = hbase_con.table('recommender:onzoom_user_updated')

columns = {'annotation': 'ant', 'click': 'click', 'gift': 'gift', 'like_event': 'like',
           'order_event': 'order', 'unlike_event': 'unlike', 'rating': 'rating', 'log_time': 'lgt', 'is_liked':'isliked'}

cf_name = 'cf'
b = table.batch()
user_b = user_table.batch()
start = time.time()
counter = 0
for row in tupleList:
    counter += 1
    cur_id = row["user_id"] + '_' + row["event_id"]
    refer_id = row["log_time"].strftime("%Y%m%d%H%M%S") + '_' + row["user_id"]
    user_b.put(refer_id, {"cf:uid".encode('utf-8'): row["user_id"].encode('utf-8')})
    user_b.put(refer_id, {"cf:eid".encode('utf-8'): row["event_id"].encode('utf-8')})
    old_data = table.row(cur_id)
    for col, hcol in columns.items():
        cur_col = (cf_name + ':' + hcol).encode('utf-8')
        val = row[col]
        if col == 'is_liked' and val is None:
            continue
        if old_data is not None and col != 'annotation' and col != 'log_time':
            if float(val) == 0.0:
                continue
            val = float(val) + float(old_data[cur_col].decode('utf-8'))
        b.put(cur_id, {cur_col: str(val).encode('utf-8')})
    if counter % 2500 == 0:
        b.send()
user_b.send()
b.send()
end = time.time()
print("Elapsed time is " + str(end - start))

#print('20200218')
#for k, v in user_table.scan(row_prefix='20200218'):
#    print(v)

#print('202002180117')
#for k, v in user_table.scan(row_prefix='202002180117'):
#    print(v)
