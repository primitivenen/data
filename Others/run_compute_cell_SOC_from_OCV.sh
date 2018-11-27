#!/bin/bash

last_date=`ls /home/dho/examples/guobiao/map_ocv_to_soc/cell_soc_from_OCV_*.done | sort | tail -1 | grep -oP '[\d]{8}'`;
last_date_agg=`ls /home/dho/examples/guobiao/map_ocv_to_soc/aggregate_cell_soc_*.done | sort | tail -1 | grep -oP '[\d]{8}'`;

if [ $last_date_agg -lt $last_date ]; then
    last_date="$last_date_agg"
fi

sdate=$(date -d "$last_date + 1 day" +"%Y%m%d");
edate=`date --date="1 day ago" +%Y%m%d`;

#for debugging
#sdate=20181108
#edate=20181109

#for regular run, it is set to false
isForceRun=false

log_file=/home/dho/examples/guobiao/map_ocv_to_soc/log/log_${sdate}_${edate}
error_file=/home/dho/examples/guobiao/map_ocv_to_soc/log/error_${sdate}_${edate}
script_dir=/home/dho/dev/guobiao/ref_scripts/

#run command
echo "runninng command:"
if [ "$isForceRun" = false ]; then
    echo "python ${script_dir}/compute_cell_SOC_from_OCV.py --dates $sdate $edate --log-file ${log_file} > $error_file 2> ${error_file}"
    python ${script_dir}/compute_cell_SOC_from_OCV.py --dates $sdate $edate --log-file $log_file > $error_file 2> $error_file
else
    echo "python ${script_dir}/compute_cell_SOC_from_OCV.py --dates $sdate $edate --log-file $log_file --force-run > $error_file 2> $error_file"
    python ${script_dir}/compute_cell_SOC_from_OCV.py --dates $sdate $edate --log-file $log_file --force-run > $error_file 2> $error_file
fi

if grep -q "Finished ${script_dir}/compute_cell_SOC_from_OCV.py --dates $sdate $edate --log-file $log_file" $error_file; then
  
    #run command
    echo "runninng command:"
    if [ "$isForceRun" = false ]; then
	echo "python ${script_dir}/aggregate_cell_SOC_records.py --dates $sdate $edate --log-file ${log_file} >> $error_file 2>> ${error_file}"
	python ${script_dir}/aggregate_cell_SOC_records.py --dates $sdate $edate --log-file $log_file >> $error_file 2>> $error_file
    else
	echo "python ${script_dir}/aggregate_cell_SOC_records.py --dates $sdate $edate --log-file $log_file --force-run >> $error_file 2>> $error_file"
	python ${script_dir}/aggregate_cell_SOC_records.py --dates $sdate $edate --log-file $log_file --force-run >> $error_file 2>> $error_file
    fi
    
    if ! grep -q "Finished ${script_dir}/aggregate_cell_SOC_records.py --dates $sdate $edate --log-file $log_file" $error_file; then
	mail -s "aggregate_cell_SOC_records failed. Check $error_file" dho@gacrndusa.com < $log_file
	echo "aggregate_cell_SOC_records failed. Check $error_file" >&2
	exit 1
    fi

else
    mail -s "compute_cell_SOC_from_OCV failed. Check $error_file" dho@gacrndusa.com < $log_file
    echo "compute_cell_SOC_from_OCV failed. Check $error_file" >&2
    exit 1
fi


if grep -q "did not finish" $log_file; then
    mail -s "exporting to Hive failed. Check $log_file" dho@gacrndusa.com < $log_file
    echo "exporting to Hive failed. Check $log_file" >&2
    exit 1
fi

if grep -q "Warning:" $log_file; then
    mail -s "There are warning message(s). Check $log_file" dho@gacrndusa.com < $log_file
fi
