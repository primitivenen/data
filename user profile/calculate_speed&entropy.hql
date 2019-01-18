select a.vin,a.start_time,a.end_time,a.distance,a.duration duration, a.distance/a.duration as speed from
	(select vin,start_time,end_time,distance,(unix_timestamp(end_time)-unix_timestamp(start_time))/3600 as duration
	 from trips_complete) a
   
select * from entropy left join
(
  select b.vin, b.total_distance, b.total_duration, b.total_distance/b.total_duration as avg_speed from
  (
	select a.vin,sum(a.distance) as total_distance,sum(a.duration) as total_duration from
	  (select vin,start_time,end_time,distance,(unix_timestamp(end_time)-unix_timestamp(start_time))/3600 as duration
	   from trips_complete) a
    group by vin
  ) b
) c
on entropy.vin=c.vin

历史201801entropy计算和整体比较
select a.vin, a.entropy, b.entropy from (select vin, sum(- probability * log2(probability)) as entropy 
from (
  select trip.vin, start_address, end_address, sum(1.0 / a.totalfrequency) as probability 
  from trip join (
	select vin, count(*) as totalfrequency from trip group by vin
  ) a 
  on trip.vin = a.vin
  where start_day>='20180101' and start_day<='20180131'
  group by trip.vin, start_address, end_address) b 
group by vin 
order by entropy desc) a left join
(select vin, sum(- probability * log2(probability)) as entropy 
from (
  select trip.vin, start_address, end_address, sum(1.0 / a.totalfrequency) as probability 
  from trip join (
	select vin, count(*) as totalfrequency from trip group by vin
  ) a 
  on trip.vin = a.vin
  group by trip.vin, start_address, end_address) b 
group by vin 
order by entropy desc) b
where a.vin=b.vin
