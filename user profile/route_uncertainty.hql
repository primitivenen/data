select vin, sum(- probability * log2(probability)) as entropy from (select trip.vin, start_address, end_address, sum(1.0 / a.totalfrequency) as probability from trip join (select vin, count(*) as totalfrequency from trip group by vin) a on trip.vin = a.vin
group by trip.vin, start_address, end_address) b group by vin order by entropy desc
