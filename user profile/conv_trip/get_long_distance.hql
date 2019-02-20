use ubi;
drop table if exists long_distance_by_day purge;
create table long_distance_by_day as
select vin, sum(long_distance) as frequency, avg(long_distance) as percent_long_distance, start_day
from 
(select vin, distance, start_day, case
  when distance >= 100 then 1
  else 0
  end as long_distance
 from conv_trips_complete) a 
group by vin, start_day
order by frequency desc