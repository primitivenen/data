
# -*- coding: utf-8 -*-

#from pyspark.sql import SparkSession
from typing import Dict
from recommender.filtering.content_filtering.utils import spark_initialization
from recommender.common.util import airflow_util
import traceback


class TableCopyJob:
    def __init__(self,
                 spark,
                 hive_tb='xxx_user_updated',
                 hive_db='ml_dw',
                 hive_src_db='dw',
                 partition_field=None,
                 record_key=None,
                 host_name='ip-10-0-15-11.ec2.internal'):
        self.spark = spark
        self.hive_tb = hive_tb
        self.hive_db = hive_db
        self.hive_src_db = hive_src_db
        self.partition_field = partition_field
        self.record_key = record_key
        self.host_name = host_name
        self.hive_server2_url: str = "jdbc:hive2://%s:10000" % host_name

    def get_target_table_s3_path(self):
        configs = airflow_util.get_configs()
        base_path: str = configs["copy_to_ml_s3"]["base_path"]
        db_path: str = configs["copy_to_ml_s3"]["db_path"]
        ml_db: str = configs["copy_to_ml_s3"]["ml_db"]
        s3_bucket_data: str = '/'.join([base_path, db_path, ml_db])
        # s3_bucket_data = "s3://nlp-xxx-recommender-ml-us-east-1/hive/"
        s3_path = '/'.join([s3_bucket_data, self.hive_tb])
        return s3_path

    def copy_to_ml_s3(self):
        table_name = self.hive_src_db + '.' + self.hive_tb
        logger.info("Process " + table_name)
        df = spark.table(table_name)
        hudiOptions = {
            'hoodie.table.name': self.hive_tb,
            'hoodie.datasource.write.recordkey.field': self.record_key,
            'hoodie.datasource.write.partitionpath.field': self.partition_field,
            'hoodie.datasource.write.precombine.field': self.partition_field,
            'hoodie.datasource.hive_sync.enable': 'true',
            'hoodie.datasource.hive_sync.database': self.hive_db,
            'hoodie.datasource.hive_sync.table': self.hive_tb,
            'hoodie.datasource.hive_sync.partition_fields': self.partition_field,
            'hoodie.datasource.hive_sync.jdbcurl': self.hive_server2_url
        }

        s3_path: str = self.get_target_table_s3_path()
        logger.info("The s3 path is: " + s3_path)
        if self.partition_field != '':
            hudiOptions.update({
                'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor'
            })
            df.write \
                .format('org.apache.hudi') \
                .option('hoodie.datasource.write.operation', 'upsert') \
                .options(**hudiOptions) \
                .mode('append') \
                .save(s3_path)
        else:
            hudiOptions.update({
                'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.NonpartitionedKeyGenerator',
                'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.NonPartitionedExtractor'
            })
            if self.hive_tb == 'xxx_user_data':
                df = df.select("user_id", "is_host", "last_login_time", "is_deleted")
            df.write \
                .format('org.apache.hudi') \
                .option('hoodie.datasource.write.operation', 'insert') \
                .options(**hudiOptions) \
                .mode('overwrite') \
                .save(s3_path)

        logger.info("Data upload for table " + table_name + " is done.")


if __name__ == '__main__':
    job_error = None
    current_job_name: str = 'copy_to_ml_s3'
    spark, config, logger, alarm, es_db = spark_initialization.init_pipeline(current_job_name)

    #spark = SparkSession \
    #    .builder \
    #    .appName("copy_data_to_ml_s3") \
    #    .config("spark.sql.catalogImplementation", "hive") \
    #    .getOrCreate()
    #configs = airflow_util.get_configs()
    host_name: str = open('./hostname.txt').read().strip('\n')
    #logger = spark._jvm.org.apache.log4j.Logger.getLogger("copy_data_to_ml_s3")

    try:
        src_db: str = config[current_job_name]['etl_db']  # 'dw'
        target_db: str = config[current_job_name]['ml_db']  # 'ml_dw'

        tb_to_recordkey: Dict[str, str] = {'xxx_user_updated': 'id',
                                           'xxx_event_data': 'event_id',
                                           'xxx_user_data': 'user_id'}

        partition_tables = ['xxx_user_updated']
        for table in partition_tables:
            TableCopyJob(spark,
                         hive_tb=table,
                         hive_db=target_db,
                         hive_src_db=src_db,
                         partition_field='create_date',
                         record_key=tb_to_recordkey[table],
                         host_name=host_name).copy_to_ml_s3()

        hudi_tables = ['xxx_event_data', 'xxx_user_data']
        for table in hudi_tables:
            TableCopyJob(spark,
                         hive_tb=table,
                         hive_db=target_db,
                         hive_src_db=src_db,
                         partition_field='',
                         record_key=tb_to_recordkey[table],
                         host_name=host_name).copy_to_ml_s3()
    except Exception as e:
        job_error = e
        error_message: str = traceback.format_exc()
        logger.error(error_message)
        alarm.collect_message('job_error', error_message, True)
    finally:
        alarm.final_post()
        spark.stop()
        if job_error:
            raise job_error


