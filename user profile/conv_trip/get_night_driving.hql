use ubi;
drop table if exists night_driving_by_day purge;
create table night_driving_by_day as
select vin, start_day, sum(night_driving_mins) as daily_night_driving_mins, 
sum(driving_mins) as daily_driving_mins, sum(night_driving_mins)/sum(driving_mins) as percent_daily_night_driving from 
(select vin, start_time, end_time, date_format(start_time,'yyyyMMdd') as start_day, (unix_timestamp(b.end_time)-unix_timestamp(b.start_time))/60 as driving_mins,  (case when b.night_end_time > b.night_start_time then (unix_timestamp(b.night_end_time)-unix_timestamp(b.night_start_time))/60 else 0 end) as night_driving_mins from (
  select vin, start_time,a.night_start, case
  when start_time > (a.night_start) then cast(start_time as timestamp)
  else cast(a.night_start as timestamp)
  end as night_start_time,
  end_time, a.night_end, case
  when end_time < (a.night_end) then cast(end_time as timestamp)
  else cast(a.night_end as timestamp)
  end as night_end_time
  from (select vin, start_time, start_day, end_time, 
		from_unixtime(unix_timestamp(CONCAT(start_day, ' ', '22:00:00'), 'yyyyMMdd HH:mm:ss')) as night_start,
	   from_unixtime(unix_timestamp(CONCAT(start_day, ' ', '05:00:00'), 'yyyyMMdd HH:mm:ss') + 86400) as night_end from conv_trips_complete) a) b) c
group by vin, start_day
order by start_day desc, daily_night_driving_mins desc