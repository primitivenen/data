use ubi;
drop table if exists trip_metrics purge;
create table trip_metrics as 
select *, 'conv' as vintype, 'conv' as subtype from conv_trip_metrics
union all
select guobiao_trip_metrics.*, 'guobiao' as vintype, vintypes.vintype as subtype  
 from guobiao_trip_metrics join vintypes 
 on guobiao_trip_metrics.vin=vintypes.vin