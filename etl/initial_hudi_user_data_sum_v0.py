
```
"""
@date 2021/04/28
Initial hudi user data table
================
"""
from pyspark.sql.functions import *
from recommender.etl.common.etl_base_job import *
from pyspark.sql import SparkSession, HiveContext
from recommender.common.util import sparksession_util as ss_util
from recommender.common.util.hudi_util import HudiUtil
from recommender.common.util import json_util
import sys
from typing import Dict
from typing import *


class UserData:
    """
        This class provides PySpark functions for data input.
    """

    def __init__(self, sql_context: SparkSession, hive_table: str = 'xxxx_user_activity_data', db: str = 'dw', hudi_table: str = 'xxxx_user_sum',
                 partition: str = "2021-02-24-01"):
        self.sql_context = sql_context
        self.hive_table = hive_table
        self.db = hive_db
        self.partition = partition
        self.output_hudi_table = hudi_table

    # null:0, true:1, false:2
    def get_xxxx_user_activity_data(self) -> List[Dict[str, str]]:
        sql = """                                                                                                                   
                select                                                                                                                         
                    t.*,
                    concat_ws('_',user_id,event_id) as id,                                                                                                                       
                    case                                                                                                                       
                        when t.like_event_time is NULL and t.unlike_event_time is NULL then 0                                               
                        when t.like_event_time is NULL and t.unlike_event_time is NOT NULL then 2                                          
                        when t.like_event_time is NOT NULL and t.unlike_event_time is NULL then 1                                           
                        when t.like_event_time > t.unlike_event_time then 1                                                                 
                        else 2                                                                                                             
                    end as is_liked,
                    case                                                                                                                       
                        when t.order_event_time is NULL and t.cancel_order_event_time is NULL then 0                                               
                        when t.cancel_order_event_time is NULL and t.order_event_time is NOT NULL then 2                                          
                        when t.cancel_order_event_time is NOT NULL and t.order_event_time is NULL then 1                                           
                        when t.cancel_order_event_time > t.order_event_time then 1                                                                 
                        else 2                                                                                                             
                    end as is_canceled_order,
                    '{}' as create_date                                                                                                    
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
                        max(case when action_type = "unlike_event" then log_time end) as unlike_event_time,
                        max(case when action_type = "order" then log_time end) as order_event_time,
                        max(case when action_type = "cancel_order" then log_time end) as cancel_order_event_time                                     
                    from {}.{} e                                                                                                                                                                                                    
                    group by                                                                                                                   
                        user_id,                                                                                                               
                        event_id,                                                                                                              
                        annotation) t                                                                                                       
               """.format(
            self.partition,
            self.db,
            self.hive_table)

        logger.info(sql)
        return self.sql_context.sql(sql)

    def insert_hudi(self, hudi_config, df):
        tmp_tb = hudi_config["default"][self.output_hudi_table]
        s3_path = tmp_tb["s3_path"]
        partition_field = tmp_tb["partition_field"]
        record_key = tmp_tb["record_key"]
        hive_db = tmp_tb["hive_db"]
        hive_tb = tmp_tb["hive_tb"]
        write_hudi_options: Dict[str, str] = {
            'hoodie.datasource.write.precombine.field': record_key
        }
        hudi_util = HudiUtil(spark=spark, hive_db=hive_db, hive_tb=hive_tb, path=s3_path,
                             partition_field=partition_field, record_key=record_key, hudi_options=write_hudi_options)
        hudi_util.update_hudi(df=df)

if __name__ == "__main__":

    spark = ss_util.get_spark_session(app_name="initial_xxxx_user_activity_sum_generator_Linda",
                                      configs={"spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                                               "spark.sql.hive.convertMetastoreParquet": "false",
                                               "spark.hadoop.hive.exec.dynamic.partition": "true",
                                               "spark.hadoop.hive.exec.dynamic.partition.mode": "nonstrict",
                                               "spark.sql.sources.partitionOverwriteMode": "dynamic"
                                               },
                                      enable_hive=True)

    logger = ss_util.get_logger(spark, "xxxx_user_activity_sum")

    config_file = "./recommender.json"
    config = json_util.load_json(config_file)
    config_etl = config.get("etl")
    hive_db = config_etl.get("hive_db")
    hive_xxxx_user_activity_data = config_etl.get("hive_xxxx_user_activity_data")
    hive_tables_schema = config_etl.get("hive_tables_schema")

    hive_xxxx_user_sum = config_etl.get("hive_hudi_xxxx_user_sum")

    data_processor = UserData(spark, hive_xxxx_user_activity_data, hive_db, hive_xxxx_user_sum)
    hive_data = data_processor.get_xxxx_user_activity_data()

    hive_data = hive_data.select('id', 'user_id', 'event_id', 'annotation', 'click', 'gift', 'like_event', 'order_event', 'unlike_event', 'rating', 'log_time', 'is_liked', 'is_canceled_order', 'create_date')
    data_processor.insert_hudi(hive_tables_schema, df=hive_data)
```
