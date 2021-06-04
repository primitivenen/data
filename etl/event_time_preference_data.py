"""
@date 2021/04/28
Generate event time preference data table
================
"""
from pyspark.sql.functions import *
from pyspark.sql import SparkSession, HiveContext, DataFrame
from recommender.common.util import sparksession_util as ss_util
from recommender.common.util.hudi_util import HudiUtil
from recommender.common.util import json_util
import sys
from typing import Dict
from typing import *


class EventTimeData:
    """
        This class provides PySpark functions for data input.
    """

    def __init__(self, sql_context: SparkSession,
                 hive_xxxx_user_activity_data_tb: str = 'xxxx_user_activity_data',
                 hive_xxxx_event_data_tb: str = 'xxxx_event_data',
                 hive_xxxx_zm_event_schedule_tb: str = 'xxxx_zm_event_schedule',
                 db: str = 'dw',
                 src_mysql: str = 'src_mysql_db',
                 hive_xxxx_event_time_preference_tb: str = 'xxxx_event_time_preference',
                 partition: str = "2021-02-24-01"):
        self.sql_context = sql_context
        self.hive_xxxx_user_activity_data = hive_xxxx_user_activity_data_tb
        self.hive_xxxx_event_data = hive_xxxx_event_data_tb
        self.hive_xxxx_zm_event_schedule = hive_xxxx_zm_event_schedule_tb
        self.db = hive_db
        self.mysql_db = src_mysql
        self.partition = partition
        self.output_table = hive_xxxx_event_time_preference_tb

    def compute_time_preference(self) -> DataFrame:
        sql = """                                                                                                                   
                with orders as (
                select 
                HOUR(FROM_UTC_TIMESTAMP(cast(d.start_time * 1000 as timestamp), timezone)) as start_hour, 
                case when category='' or category = 'Other' then 'Other' else category end category, 
                e.product as event_id,
                case when a.action_val is not NULL then a.action_val else 0 end as orders
                from {}.{} e left outer join {}.{} a 
                on e.product = a.event_id and a.action_type = 'order'
                inner join {}.{} d on e.product = d.event_id and d.access_type = 1 and d.event_status = 8 and d.moderation_res=1 and d.sale_end_time <= unix_timestamp()
                )
                select concat_ws('_', category, cast(start_hour as string)) as id, start_hour, category, sum(orders) * 1.0 / count(distinct event_id) as mean_orders
                from orders 
                group by id, start_hour, category                                                                                                    
               """.format(
            self.mysql_db,
            self.hive_xxxx_zm_event_schedule,
            self.db,
            self.hive_xxxx_user_activity_data,
            self.db,
            self.hive_xxxx_event_data)

        logger.info(sql)
        return self.sql_context.sql(sql)

    def insert_hudi(self, hudi_config, df):
        print(self.output_table)
        tmp_tb = hudi_config["default"][self.output_table]
        print(tmp_tb)
        s3_path = tmp_tb["s3_path"]
        partition_field = tmp_tb["partition_field"]
        record_key = tmp_tb["record_key"]
        hive_db = tmp_tb["hive_db"]
        hive_tb = tmp_tb["hive_tb"]
        write_hudi_options: Dict[str, str] = {
            'hoodie.datasource.write.precombine.field': record_key
        }
        hudi_util = HudiUtil(spark=spark, hive_db="default", hive_tb=hive_tb, path=s3_path,
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

    logger = ss_util.get_logger(spark, "event_time_preference_data")

    config_file = "./recommender.json"
    config = json_util.load_json(config_file)
    config_etl = config.get("etl")
    hive_db = config_etl.get("hive_db")
    src_mysql_db = config_etl.get("src_mysql_db")
    hive_xxxx_user_activity_data = config_etl.get("hive_xxxx_user_activity_data")
    hive_xxxx_event_data = config_etl.get("hive_xxxx_event_data")
    hive_xxxx_zm_event_schedule =  config_etl.get("hive_xxxx_zm_event_schedule")
    hive_tables_schema = config_etl.get("hive_tables_schema")

    hive_xxxx_event_time_preference = config_etl.get("hive_event_time_preference")
    sc = spark.sparkContext
    sqlContext = HiveContext(sc)
    data_processor = EventTimeData(sqlContext,
                              hive_xxxx_user_activity_data,
                              hive_xxxx_event_data,
                              hive_xxxx_zm_event_schedule,
                              hive_db,
                              src_mysql_db,
                              hive_xxxx_event_time_preference)
    hive_data = data_processor.compute_time_preference()

    hive_data = hive_data.select('id', 'start_hour', 'category', 'mean_orders')
    data_processor.insert_hudi(hive_tables_schema, df=hive_data)
