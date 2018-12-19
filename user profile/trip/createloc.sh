cd /home/wchen/dsc

for filename in trip_v3_*.py; do
    for (( i=0; i<=9; i++ )); do
	python $filename --location >$(basename "$filename" .py)_Log$i.out
    done
done

echo "Location Created."
