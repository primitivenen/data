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
