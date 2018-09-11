#!/bin/bash
cd /home/wchen/dsa/
python guobiao_data_0815.py   |& tee -a /home/wchen/dsa/loglog 
bash batch_insert_guobiao_orc.sh  |& tee -a /home/wchen/dsa/loglog
hive -f get_high_cell_volt_diff_all_records.hql  |& tee -a /home/wchen/dsa/loglog
hive -f get_high_cell_volt_diff_by_vin.hql  |& tee -a /home/wchen/dsa/loglog