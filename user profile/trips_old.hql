use guobiao_tsp_tbls;
drop table if exists starts_redundant purge;
create table starts_redundant as select vin, loc_lat, loc_lon, bigint(ts / 1000) as ts_seconds, day, veh_odo 
from guobiao_raw_orc
where veh_st = 1;

drop table if exists starts_latest purge;
create table starts_latest as select vin, veh_odo, max(ts_seconds) as ts_seconds  
from starts_redundant group by vin, veh_odo;

drop table if exists starts purge;
create table starts as select starts_redundant.* 
from starts_redundant inner join starts_latest 
on starts_redundant.vin = starts_latest.vin and starts_redundant.veh_odo == starts_latest.veh_odo and starts_redundant.ts_seconds = starts_latest.ts_seconds;

drop table if exists ends_redundant purge;
create table ends_redundant as select vin, loc_lat, loc_lon, bigint(ts / 1000) as ts_seconds, day, veh_odo 
from guobiao_raw_orc
where veh_st = 2;

drop table if exists ends_latest purge;
create table ends_latest as select vin, veh_odo, max(ts_seconds) as ts_seconds  
from ends_redundant group by vin, veh_odo;

drop table if exists ends purge;
create table ends as select ends_redundant.* 
from ends_redundant inner join ends_latest 
on ends_redundant.vin = ends_latest.vin and ends_redundant.veh_odo == ends_latest.veh_odo and ends_redundant.ts_seconds = ends_latest.ts_seconds;

drop table if exists trip_candidates purge;
create table trip_candidates as select starts.vin, starts.loc_lat as start_loc_lat, starts.loc_lon as start_loc_lon, from_utc_timestamp(to_utc_timestamp(from_unixtime(starts.ts_seconds), "America/Los_Angeles"), "Asia/Shanghai") as start_time, starts.day as start_day, ends.loc_lat as end_loc_lat, ends.loc_lon as end_loc_lon, from_utc_timestamp(to_utc_timestamp(from_unixtime(ends.ts_seconds), "America/Los_Angeles"), "Asia/Shanghai") as end_time, ends.veh_odo - starts.veh_odo as distance
from starts inner join ends on starts.vin = ends.vin
where ends.veh_odo > starts.veh_odo and ends.ts_seconds > starts.ts_seconds and ends.ts_seconds - starts.ts_seconds < 100000 and ends.veh_odo - starts.veh_odo < 2000;

drop table if exists trip_distance purge;
create table trip_distance as select vin, start_time, min(distance) as distance 
from trip_candidates group by vin, start_time;

