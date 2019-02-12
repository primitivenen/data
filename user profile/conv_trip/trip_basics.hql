select count(*) from
(
select vin,max(distance) as max_distance,min(distance) as min_distance, max(speed) as max_speed,min(speed) as min_speed,
max(duration) as max_duration, min(duration) as min_duration 
from conv_trips_complete
group by vin) a
where max_duration>50
