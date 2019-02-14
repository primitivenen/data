select vin, sum(- probability * log2(probability)) as entropy from (select trip.vin, start_address, end_address, sum(1.0 / a.totalfrequency) as probability from trip join (select vin, count(*) as totalfrequency from trip group by vin) a on trip.vin = a.vin
group by trip.vin, start_address, end_address) b group by vin order by entropy desc

select vin, sum(- probability * log2(probability)) as entropy_all 
from (select vin, ROUND(CAST(start_loc_lat as float), 3) as start_loc_lat, 
	  ROUND(CAST(start_loc_lon as float), 3) as start_loc_lon, 
	  ROUND(CAST(end_loc_lat as float), 3) as end_loc_lat, 
	  ROUND(CAST(end_loc_lon as float), 3) as end_loc_lon, 
	  sum(1.0 / a.totalfrequency) as probability 
from conv_trips_complete join (select vin, count(*) as totalfrequency from conv_trips_complete group by vin) a 
on conv_trips_complete.vin = a.vin group by conv_trips_complete.vin, start_loc_lat, start_loc_lon, end_loc_lat, end_loc_lon) b group by vin order by entropy desc

select c.vin, entropy_all, entropy_last_month from
(
select vin, sum(- probability * log2(probability)) as entropy_all 
from (select vin, ROUND(CAST(start_loc_lat as float), 3) as start_loc_lat, 
	  ROUND(CAST(start_loc_lon as float), 3) as start_loc_lon, 
	  ROUND(CAST(end_loc_lat as float), 3) as end_loc_lat, 
	  ROUND(CAST(end_loc_lon as float), 3) as end_loc_lon, 
	  sum(1.0 / a.totalfrequency) as probability 
from conv_trips_complete join (select vin, count(*) as totalfrequency from conv_trips_complete group by vin) a 
on conv_trips_complete.vin = a.vin group by conv_trips_complete.vin, start_loc_lat, start_loc_lon, end_loc_lat, end_loc_lon) b 
group by vin order by entropy desc
) c join
(
select vin, sum(- probability * log2(probability)) as entropy_last_month 
from 
(select vin, ROUND(CAST(start_loc_lat as float), 3) as start_loc_lat, 
	  ROUND(CAST(start_loc_lon as float), 3) as start_loc_lon, 
	  ROUND(CAST(end_loc_lat as float), 3) as end_loc_lat, 
	  ROUND(CAST(end_loc_lon as float), 3) as end_loc_lon, 
	  sum(1.0 / a.totalfrequency) as probability 
 from conv_trips_complete join (select vin, count(*) as totalfrequency from conv_trips_complete group by vin) a 
 on conv_trips_complete.vin = a.vin 
 where conv_trips_complete.start_day>=from_unixtime(unix_timestamp(date_sub('2019-02-14', 30), 'yyyy-MM-dd'))
 group by conv_trips_complete.vin, start_loc_lat, start_loc_lon, end_loc_lat, end_loc_lon
) b 
group by vin order by entropy desc
) d
on c.vin=d.vin
