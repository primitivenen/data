cd /home/wchen/collect

for filename in `ls -a`; do
	mv $filename trips_complete.json
	curl -H Content-Type:application/x-ndjson --user elastic:hfzdXseEdHVFUq86Y2U3 -XPOST localhost:9200/_bulk?pretty --data-binary @trips_complete.json
	rm trips_complete.json 
done