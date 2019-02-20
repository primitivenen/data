use ubi;
drop table if exists conv_trips_complete purge;
create table conv_trips_complete as
select vin, start_time, start_lat as start_loc_lat, start_lon as start_loc_lon,
end_time, end_lat as end_loc_lat, end_lon as end_loc_lon, distance, duration, speed,
from_unixtime(unix_timestamp(CAST(TO_DATE(start_time) as date), 'yyyy-MM-dd'),'yyyyMMdd') as start_day
from
(select vin, from_unixtime(cast(start_time/1000000000 as bigint)) as start_time, start_lat, start_lon, 
from_unixtime(cast(end_time/1000000000 as bigint)) as end_time,  
end_lat, end_lon, distance, duration, speed
from 
(select b.* from 
(select conv_trips.* from ubi.conv_trips join 
(select vin, start_time, max(end_time) as end_time from conv_trips group by vin, start_time) a 
on conv_trips.vin = a.vin and conv_trips.start_time = a.start_time and conv_trips.end_time = a.end_time) b
join 
(select vin, end_time, min(start_time) as start_time from conv_trips group by vin, end_time) c 
on b.vin = c.vin and b.start_time = c.start_time and b.end_time = c.end_time) e
) f