select h.*, g.entropy 
from entropy g join
(
select e.vin, e.start_day, e.daily_night_driving_mins, e.percent_daily_night_driving,
long_distance_count, percent_long_distance, percent_am_peak, am_peak_mins, percent_pm_peak, pm_peak_mins
from night_driving_by_day e join
(
select c.vin, c.start_day, c.frequency as long_distance_count, c.percent_long_distance, percent_am_peak, 
am_peak_mins, percent_pm_peak, pm_peak_mins
from long_distance_by_day c join
(
select a.vin, a.start_day, a.percent_daily_peak_driving as percent_am_peak, a.daily_peak_driving_mins as am_peak_mins, 
b.percent_daily_peak_driving as percent_pm_peak, b.daily_peak_driving_mins as pm_peak_mins
from am_peak_driving_by_day a join pm_peak_driving_by_day b
on a.vin=b.vin and a.start_day=b.start_day) d
on c.vin=d.vin and c.start_day=d.start_day) f
on e.vin=f.vin and e.start_day=f.start_day) h
on h.vin = g.vin


