import csv
import os
inputFileName = "C:\\Users\\Wan Chen\\Desktop\\test\\StatFinal_20180515.csv"
outputFileName = os.path.splitext(inputFileName)[0] + "_modified.csv"

with open(inputFileName,'rb') as infile, open(outputFileName,'wb') as outfile:
    r=csv.reader(infile)
    w=csv.writer(outfile)
    i=next(r)
    o=[]
    for name in i:        
        o.append(name.replace('.','_'))
    w.writerow(o)
    for row in r:
        w.writerow(row)
        
        