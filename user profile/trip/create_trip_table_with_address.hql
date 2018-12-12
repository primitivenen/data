select * from trips_complete t1 left join (select b.address, b.loc_latitude, b.loc_longtitude as start_address from trips_complete a left join address b
on cast(a.start_loc_lat as decimal(10,3))=b.loc_latitude and cast(a.start_loc_lon as decimal(10,3))=b.loc_longtitude) t2
on cast(t1.start_loc_lat as decimal(10,3))=t2.loc_latitude and cast(t1.start_loc_lon as decimal(10,3))=t2.loc_longtitude) 
