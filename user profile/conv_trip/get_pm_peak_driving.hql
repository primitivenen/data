use ubi;
drop table if exists pm_peak_driving_by_day purge;
create table pm_peak_driving_by_day as
select vin, start_day, sum(peak_driving_mins) as daily_peak_driving_mins, 
sum(driving_mins) as daily_driving_mins, sum(peak_driving_mins)/sum(driving_mins) as percent_daily_peak_driving from 
(select vin, start_time, end_time, date_format(start_time,'yyyyMMdd') as start_day, (unix_timestamp(b.end_time)-unix_timestamp(b.start_time))/60 as driving_mins,  
 (case when b.peak_end_time > b.peak_start_time then (unix_timestamp(b.peak_end_time)-unix_timestamp(b.peak_start_time))/60 else 0 end) as peak_driving_mins from (
  select vin, start_time,a.peak_start, case
  when start_time > (a.peak_start) then cast(start_time as timestamp)
  else cast(a.peak_start as timestamp)
  end as peak_start_time,
  end_time, a.peak_end, case
  when end_time < (a.peak_end) then cast(end_time as timestamp)
  else cast(a.peak_end as timestamp)
  end as peak_end_time
  from (select vin, start_time, start_day, end_time, 
		from_unixtime(unix_timestamp(CONCAT(start_day, ' ', '16:00:00'), 'yyyyMMdd HH:mm:ss')) as peak_start,
	   from_unixtime(unix_timestamp(CONCAT(start_day, ' ', '19:00:00'), 'yyyyMMdd HH:mm:ss')) as peak_end 
		from conv_trips_complete) a) b) c
group by vin, start_day
order by start_day desc, daily_peak_driving_mins desc