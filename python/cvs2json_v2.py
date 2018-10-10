import csv
import json
import glob
import os
import sys

filename = "C:\\Users\\WAA\\Downloads\\ge3_core_stats.csv"
jsonfile = filename.split('.')[0] + '.json'
shortname = os.path.basename(filename).split('.')[0]

def isFloat(s):
    try:
        float(s)
        return True
    except ValueError:
        return False
        
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
                
        json.dump(op_dict, f, separators=(',', ':'), ensure_ascii = False)
        f.write("\n")
        for k in r:
            if isinstance(r[k], dict) == False:
                if(r[k].isdigit()):
                    r[k] = int(r[k])
                elif (isFloat(r[k])):
                    r[k] = float(r[k])
        for i in list(r):
            if not r[i] or r[i]=='':
                r.pop(i, None)
        json.dump(r, f, separators=(',', ':'))
        f.write("\n")
