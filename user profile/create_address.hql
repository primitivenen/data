DROP TABLE IF EXISTS ubi.address PURGE;
CREATE EXTERNAL TABLE ubi.address(
	loc_latitude double, 
	loc_longtitude double, 
	address string
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/user/hive/warehouse/address';
