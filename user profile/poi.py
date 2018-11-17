from pymongo import MongoClient
import pandas as pd
import numpy as np
import urllib2
import json
import warnings
warnings.filterwarnings('ignore')

from retry import *

@retry(urllib2.URLError, tries=5, delay=3, backoff=2)
def urlopen_with_retry(url):
    return urllib2.urlopen(url)

# TODO: add creation Timestamp, retention policy
def build_urls(db, lats, lons):
    urls = []
    url_base = "https://restapi.amap.com/v3/geocode/regeo?batch=true&extensions=all&radius=3000&roadlevel=0&\
key=27522a3d9da8f4e80d6580c80d010d4c&location="
    
    url = url_base
    count = 0
    for lat, lon in zip(lats, lons):
        if count > 0 and count % 20 == 0:
            urls.append(url[:-1])
            url = url_base
        if lat == 0 or lon == 0:
            continue
        #if np.isnan(lat) or np.isnan(lon):
         #   continue
        lon=float(lon)
        lat=float(lat)
        key = '{:.6f},{:.6f}'.format(lon, lat)
        url = url + key + '|'
        count += 1

    if '|' in url:
        urls.append(url[:-1])
    
    return urls

#def findPOI(db, lat, lon):
	


def main():
    connection = MongoClient("mongodb://172.15.7.43:27017/admin", username='dev', password='dev')
    db = connection.gps
    print('loading data..')
    
    data = pd.read_csv('gps.csv', header=None)
    data.columns = ['lat', 'lon']
    data = data.drop_duplicates()
    lats = data['lat'].tolist()
    lons = data['lon'].tolist()
    
    #start = 3000000
    #x = 3000000
    #a = lats[start:x]
    #b = lons[start:x]
    print('building urls..')
    #urls = build_urls(db, a, b)
    urls = build_urls(db, lats, lons)
    
    print('rev geocoding..')
    for i, url in enumerate(urls):
        #keys = url.split('=')[-1].split('|')
        response = urlopen_with_retry(url)
        res = json.loads(response.read())
        
        #print(res)
        
        if res[u'info'] != 'OK':
            continue
        adds = res[u'regeocodes']
        #if len(keys) != len(adds):
        #    raise Exception('number of gps keys {} do not match number of reverse geocoding result {}'.\
        #                    format(len(keys), len(adds)))
        for add in adds:
	   ll=add[u'pois']
	   documents = []
	   for val in ll:
	       cursor = db.pois.find({'_id': {'$eq': val[u'id']}})
	       count=0
	       for doc in cursor:
	           count+=1
			#matches=cursor.count_documents()
	       if count==0:
		   documents.append({'_id':val[u'id'], 'poi': val})
				#print(val[u'id'])
				#print('----------end')
	   if len(documents)!=0:
	       db.pois.insert_many(documents)
        
        if i % 5 == 0: # every 100 gps points
            print('progress: {}/{}'.format(i, len(urls)))
    
    print('done')
if __name__ == '__main__':
    main()
