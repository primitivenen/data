import csv
import json
import glob
import os

filename = "C:\\Users\\Wan Chen\\Desktop\\script test\\recalls.csv"
jsonfile = filename.split('.')[0] + '.json'
shortname = os.path.basename(filename).split('.')[0]

with open(filename) as f:
    reader = csv.DictReader(f)
    rows = list(reader)

op_dict = {"index": {"_index": shortname, "_type": 'doc'}}

variables = ['start', 'mean', 'end']
  
with open(jsonfile, 'w') as f:
    for r in rows:
        for var in variables:
            lat = var + '_latitude'
            lon = var + '_longitude' 
            if(lat in r and lon in r) :
                if (r[lat] != '' and r[lon] != '') :
                    r[var + '_location'] =  {"lat" : float(r[lat]), "lon" : float(r[lon])}

                r.pop(lat, None)
                r.pop(lon, None)
                
        json.dump(op_dict, f, separators=(',', ':'))
        f.write("\n")
        json.dump(r, f, separators=(',', ':'))
        f.write("\n")
        
