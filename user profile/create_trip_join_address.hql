统一替换文件里的\t为,   (txt转化为csv)
sed -i -e 's/\t/,/g' newfile

select t1.*, t2.address as end_address from (select a.*, b.address as start_address from trips_complete a left join address b
on round(cast(a.start_loc_lat as float), 3)=round(cast(b.loc_latitude as float), 3) and 
round(cast(a.start_loc_lon as float), 3) = round(cast(b.loc_longtitude as float), 3)) t1 left join address t2
on round(cast(t1.end_loc_lat as float), 3)=round(cast(t2.loc_latitude as float), 3) and 
round(cast(t1.end_loc_lon as float), 3) = round(cast(t2.loc_longtitude as float), 3)
