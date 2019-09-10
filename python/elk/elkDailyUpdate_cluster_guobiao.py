import findspark
findspark.init("/usr/hdp/current/spark2-client")
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
import subprocess
import csv
import json
import os
import sys
import shutil
import glob

SET_1 = ["guobiao_charging", "guobiao_daily_stats", "guobiao_trips_on_battery"] # "guobiao_hourly_stats""guobiao_charging_full_soc""guobiao_trips_full_soc"
SET_2 = ["ge3_core_stats", "ge3_selection_agg_cell_soc", "ge3_selection_cell_soc"] #"mileage_model_bytrip_soh"
SET_3 = [] #"mileage_model_monthly_soh"
SET_4 = [] #"guobiao_vin_ranking_ah"
        
#VM_CREDENTIALS = "bitnami@172.15.5.29"
ES_CREDENTIALS = "elastic:xxxxxxx"
LOCAL_DIRECTORY = "/home/elk-daily-updates/"
TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"
ES_IP_PORT = "localhost:9200/"
#PATH_TO_MAPPINGS = "/home/bitnami/elk-daily-updates/mappings/"
#UPLOAD_PATH = "/home/bitnami/elk-daily-updates/data/"
NOT_UPLOADED_PATH = "/home/elk-daily-updates/data_not_uploaded/"
TOTAL_LINES_IN_EACH_FILE = "10000"
PREFIX = "XX"

DATABASE_NAME = "guobiao_tsp_tbls."

def queryNewData(indexName, hc):
    fileName = indexName + "_lastSuccessfulDate.txt"
    
    with open(LOCAL_DIRECTORY + fileName, "r") as f:
        lastSuccessfulDate = f.readline().strip()
    
    if (indexName in SET_1):
        tableName = DATABASE_NAME + concatenateListElements(indexName.split("_")[1:])
        query_1 = """SELECT t1.*, t2.vintype FROM {} t1 LEFT OUTER JOIN guobiao_tsp_tbls.vintypes t2 ON (t1.vin = t2.vin) WHERE t1.day > '{}'""".format(tableName, lastSuccessfulDate)
    elif (indexName in SET_2):
        tableName = DATABASE_NAME + indexName
        query_1 = """SELECT * FROM {} WHERE day > '{}'""".format(tableName, lastSuccessfulDate)
    elif (indexName in SET_3):
        tableName = DATABASE_NAME + indexName
        query_1 = """SELECT * FROM {} WHERE month_date > '{}'""".format(tableName, lastSuccessfulDate)
    elif (indexName in SET_4):
        tableName = DATABASE_NAME + concatenateListElements(indexName.split("_")[1:])
        query_1 = """SELECT t1.*, t2.lat, t2.lon, t2.city, t2.province, t2.city1, t2.province1 FROM {} t1 LEFT OUTER JOIN guobiao_tsp_tbls.veh_locs t2 ON (t1.vin = t2.vin) WHERE t1.day > '{}'""".format(tableName, lastSuccessfulDate)
    
    #import pdb; pdb.set_trace()
    
    queryResult_1 = hc.sql(query_1)
        
    totalNewRecords = queryResult_1.count()
    print("INFO: Fetched " + str(totalNewRecords) + " new records for index " + indexName + ".")
        
    if (totalNewRecords > 0):
        if ((indexName in SET_1) or (indexName in SET_2) or (indexName in SET_4)):
            query_2 = """SELECT MAX(day) FROM {}""".format(tableName)
        elif (indexName in SET_3):
            query_2 = """SELECT MAX(month_date) FROM {}""".format(tableName)
		
        queryResult_2 = hc.sql(query_2)
        df_2 = queryResult_2.toPandas()
            
        dateString = df_2.iloc[0].iloc[0].strftime("%Y-%m-%d")
        hdfsPathString = indexName + "-" + dateString
        localPathString = LOCAL_DIRECTORY + hdfsPathString + ".csv"
            
        try:
            queryResult_1.write.csv(hdfsPathString, timestampFormat = TIMESTAMP_FORMAT)
        except Exception as ex:
            print(ex)
            print("WARN: Unable to write results of the Hive query into hdfs, skipping updates.")
            return
                    
        (ret, out, err) = run_cmd(["hdfs", "dfs", "-getmerge", hdfsPathString, localPathString])
        if (ret != 0):
            print("WARN: Unable to merge results from hdfs into local directory, skipping updates.")
            return
            
        # No need for error checking here
        run_cmd(["hdfs", "dfs", "-rm", "-r", "-skipTrash", hdfsPathString])
        run_cmd(["rm", LOCAL_DIRECTORY + "." + hdfsPathString + ".csv" + ".crc"])
            
        # Create the index in elasticsearch
        print("INFO: Creating the index " + hdfsPathString + " in elasticsearch.")
        (ret, out, err) = run_cmd(["curl", "-H", "Content-Type:application/json", "--user", 
                                   ES_CREDENTIALS, "-XPUT", ES_IP_PORT + hdfsPathString, "-d", 
                                   "@" + LOCAL_DIRECTORY + indexName + ".json"])
            
        if (ret != 0):
            print("ERROR: Failed to create new index " + hdfsPathString + " in elasticsearch, skipping the update.")
            return
            
        run_cmd(["split", "-l", TOTAL_LINES_IN_EACH_FILE, localPathString, LOCAL_DIRECTORY + hdfsPathString + "-" + PREFIX])
            
        filesToUpload = glob.glob(LOCAL_DIRECTORY + hdfsPathString + "-" + PREFIX + "*")
                        
        for f in filesToUpload:
            run_cmd(["mv", f, f + ".csv"])
                
            # Add header
            with open(LOCAL_DIRECTORY + "tempFile.csv", "w") as outfile:
                for infile in (LOCAL_DIRECTORY + indexName + ".header", f + ".csv"):
                    shutil.copyfileobj(open(infile), outfile)
                
            run_cmd(["mv", LOCAL_DIRECTORY + "tempFile.csv", f + ".csv"])
                
            # Convert to required format for upload, creates a json file
            csv2es(hdfsPathString, f + ".csv")
                
            # Upload the new data to elasticsearch
            (ret, out, err) = run_cmd(["curl", "-H", "Content-Type:application/x-ndjson", "--user", 
                                       ES_CREDENTIALS, "-XPOST", ES_IP_PORT + "_bulk?pretty", "--data-binary", 
                                       "@" + LOCAL_DIRECTORY + f.split("/")[-1] + ".json"])
                            
            if ('"errors" : false,' in out):
                run_cmd(["rm", LOCAL_DIRECTORY + f.split("/")[-1] + ".json"])
                print("INFO: Bulk " + f.split("/")[-1] + " uploaded successfully to elasticsearch.")
            else:
                run_cmd(["mv", LOCAL_DIRECTORY + f.split("/")[-1] + ".json", 
                         NOT_UPLOADED_PATH + f.split("/")[-1] + ".json"])
                text_file = open(NOT_UPLOADED_PATH + f.split("/")[-1] + ".txt", "w")
                text_file.write("%s" % out)
                text_file.close()
                print("WARN: Bulk " + f.split("/")[-1] + " was not uploaded, or was uploaded partially.")
                    
            # Clean up
            run_cmd(["rm", f + ".csv"])
                        
        run_cmd(["rm", LOCAL_DIRECTORY + hdfsPathString + ".csv"])
            
        df_2.to_csv(LOCAL_DIRECTORY + fileName, index = False, header = False)
        print("INFO: Updated the lastSuccessfulDate for index " + indexName + ".")
        

def run_cmd(args_list):
    # Run linux commands
    #print("Running system command: {0}".format(" ".join(args_list)))
    proc = subprocess.Popen(args_list, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return = proc.returncode
    return s_return, s_output, s_err


def isFloat(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def concatenateListElements(a_list):
    result = ""
    for element in a_list:
        result = result + element + "_"
    return result[0:-1]


def csv2es(indexName, fileName):
    #LOCAL_DIRECTORY = "/home/elk-daily-updates/"
    
    jsonFileName = fileName.split("/")[-1].split(".")[0] + ".json"
    
    with open(fileName) as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    metaData = {"index": {"_index": indexName, "_type": "doc"}}

    geoVariables = ["start", "mean", "end", "vehicle"]
  
    with open(LOCAL_DIRECTORY + jsonFileName, "w") as f:
        for r in rows:
            for var in geoVariables:
                lat = var + "_latitude"
                lon = var + "_longitude" 
                if(lat in r and lon in r):
                    if (r[lat] != "" and r[lon] != ""):
                        r[var + "_location"] =  {"lat": float(r[lat]), "lon": float(r[lon])}

                    r.pop(lat, None)
                    r.pop(lon, None)
                
            json.dump(metaData, f, separators = (",", ":"), ensure_ascii = False)
            f.write("\n")
            for k in r.iterkeys():
                if isinstance(r[k], dict) == False:
                    if(r[k].isdigit()):
                        r[k] = int(r[k])
                    elif (isFloat(r[k])):
                        r[k] = float(r[k])
            for i in list(r):
                if r[i] == '':
                    r.pop(i, None)
            json.dump(r, f, separators = (",", ":"))
            f.write("\n")

# Check the status of the elasticsearch server
(ret, out, err) = run_cmd(["curl", "--user", ES_CREDENTIALS, "-XGET", ES_IP_PORT + "_cat/indices?v"])

if (ret == 0):
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("ELK Daily Update - Guobiao") \
        .enableHiveSupport() \
        .config("spark.executor.memory", "4G") \
        .config("spark.submit.deployMode", "client") \
        .config("spark.memory.fraction", "0.7") \
        .config("spark.executor.cores", "4") \
        .config("spark.executor.instances", "10") \
        .config("spark.yarn.am.memory", "4G") \
        .getOrCreate()
        
    sc = spark.sparkContext
    hiveContext = HiveContext(sc)
    
    updates = SET_1 + SET_2 + SET_3 + SET_4
    
    #updates = ["ge3_core_stats"]
    
    i = 0
    
    while i < len(updates):
        # clean up in case the previous run didn't complete
        (ret, out, err) = run_cmd(["find", LOCAL_DIRECTORY, "-maxdepth", "1", "-name", updates[i] + "*", "-perm", "644"])
        filesToDelete = out.split("\n")[:-1]
        for f in filesToDelete:
            run_cmd(["rm", f])
        
        print("INFO: Updating the index " + updates[i] + ".")
        queryNewData(updates[i], hiveContext)
        i = i + 1

    spark.stop()
    
else:
    print("ERROR: elasticsearch server is not running, skipping updates.")
