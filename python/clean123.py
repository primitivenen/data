import os

import pandas as pd
def epoch_time_converter(t):
    t = str(t)
    if len(t) == 13:
        unit = 'ms'
    elif len(t) == 10:
        unit = 's'
    else:
        #raise AttributeError('{} is not in valid epoch time format')
        return 'BadTimeStamp'
    ts = pd.to_datetime(t, unit=unit, utc=True).tz_convert('Asia/Hong_Kong')
    return ts.strftime('%Y%m%d')
    
dirname = '/gaei/gacrnd/data/guobiao/csv/'

newFileDict = {}
for dateDir in os.listdir(dirname):
    num = long(dateDir[2:])
    print num

    if num>=20170630 and num <=20180212:
        dateDirPath = os.path.join(dirname, dateDir)
        if os.path.isdir(dateDirPath):
            for filename in os.listdir(dateDirPath):
                if not filename.endswith('_SUCCESS'):
                    filepath = os.path.join(dateDirPath, filename)
                    if os.path.isfile(filepath):
                        with open(filepath, 'r') as infile:
                            for line in infile:
                                fields=line.split(',')
                                day = epoch_time_converter(fields[2])
                                out_file = '{}_new.csv'.format(day)
                                newfilename = os.path.join(dirname, out_file) 
                                if not newfilename in newFileDict:
                                    newFileDict[newfilename] = open(newfilename, 'a')
                                newFileDict[newfilename].write(line)
                        print (filepath + ' is done')
                    
            
for key, value in newFileDict.items() :
    value.close()
