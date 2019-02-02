select vin, min_rate, max_rate, std_rate, sum_rate/count_rate as avg_rate from 
(
  select vin, min(rate) as min_rate, max(rate) as max_rate, std(rate) as std_rate, 
  sum(rate) as sum_rate, count(rate) as count_rate from
	(
	select vin, start_day, count(*) as rate from trips_complete
	group by vin, start_day
	order by vin
	) a
  group by vin
) b
