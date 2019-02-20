cd /home/wchen/dsc

start_date=$(date -d "20170925" +%Y%m%d)
end_date=$(date -d "20181231" +%Y%m%d)
d=$start_date
while [[ "$d" -le $end_date ]]; do
	echo "Processing $d;"
	d=$start_date
	d1=$(date --date="$start_date+2 days" +%Y%m%d)
	python conv_trip4.py --all $d $d1
	start_date=$d1
done
