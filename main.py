# coding = utf8

### LIBRARIES ---------------------------------------
import urllib3
import json
import os
import pyspark
import sys

from dotenv import load_dotenv, find_dotenv
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

### .ENV VARIABLES ---------------------------------------
env_file = find_dotenv('.env')
load_dotenv(env_file)

user = os.environ.get('MONGODB_USER')
pwd = os.environ.get('MONGODB_PASSWORD')
host = os.environ.get('MONGODB_HOST')
port = os.environ.get('MONGODB_PORT')
database = os.environ.get('MONGODB_DATABASE')
collection = os.environ.get('MONGODB_COLLECTION')
dbauth = os.environ.get('MONGODB_AUTH')

### FUNCTIONS ---------------------------------------
def fn_progress_bar(count_value, total, suffix=''):

    bar_length = 100
    filled_up_Length = int(round(bar_length * count_value / float(total)))
    percentage = round(100.0 * count_value / float(total), 1)
    bar = '=' * filled_up_Length + '-' * (bar_length - filled_up_Length)
    sys.stdout.write('[%s] %s%s ...%s\r' %(bar, percentage, '%', suffix))
    sys.stdout.flush()

def fn_request_pokemon_api():

    http = urllib3.PoolManager()

    print(':: Request API. Getting Data!')

    ## GET DATA ---------------------------------------
    try:
        uri = f'https://pokeapi.co/api/v2/pokemon/'
        
        # Total registry
        request_count_registry = http.request('GET', uri)
        response_count_registry = json.loads(request_count_registry.data.decode('utf-8'))

        if response_count_registry['count'] == 0 or\
            response_count_registry['count'] == '' or\
                response_count_registry['count'] == None:
            None

    except Exception as error:
        print(error)
        pass
    
    data = []

    for i in range(response_count_registry['count']):
        
        i = i + 1

        try:
            fn_progress_bar(i, 1300)            
    
            uri = f'https://pokeapi.co/api/v2/pokemon/{i}/'
            
            request = http.request('GET', uri)

            response = json.loads(request.data.decode('utf-8'))

            if response == None or response == '':
                None

            else:
                data.append(response)
                # print(response)

        except Exception as error:
            # print(error)
            pass
    
    print('\n:: Data collected. View data using \'<dataframe>.show()\'!')

    return data

def fn_init_spark(mongodb_uri):

    conf = SparkConf()
    
    # conf.set('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.13:10.2.0')\
    # conf.set('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector:10.0.2')\

    conf.setMaster('local[*]')\
        .setAppName('lab-pyspark')
    
    conf.set('spark.driver.memory', '16g')
    conf.set('spark.worker.cleanup.enabled', 'true')
    conf.set('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:10.2.0')
    conf.set('spark.SparkContext.setLogLevel', 'OFF')
    conf.set('spark.sql.legacy.json.allowEmptyString.enabled', 'true')
    conf.set('spark.sql.jsonGenerator.ignoreNullFields', 'true')

    # Set up read connection :
    conf.set('spark.mongodb.read.connection.uri', mongodb_uri)
    conf.set('spark.mongodb.read.database', database)
    conf.set('spark.mongodb.read.collection', collection)

    # Set up write connection
    conf.set('spark.mongodb.write.connection.uri', mongodb_uri)
    conf.set('spark.mongodb.write.database', database)
    conf.set('spark.mongodb.write.collection', collection)
    conf.set('spark.mongodb.write.ignoreNullValues', 'true')
    conf.set('spark.mongodb.write.convertJson', 'any')
    conf.set('spark.mongodb.write.operationType', 'replace')
    conf.set('spark.mongodb.write.outputExtendedJson', 'true')
    conf.set('spark.mongodb.write.sampleSize', 50000)

    sc = SparkContext.getOrCreate(conf = conf)

    ss = SparkSession(sc).builder\
                         .getOrCreate()
    
    print(':: Spark Session Initiated. Check status!')

    return sc, ss

def fn_mongodb_connection():

    mongodb_uri = f'mongodb+srv://{user}:{pwd}@{host}/?{dbauth}'

    mongodb_conn = MongoClient(mongodb_uri, 
                               server_api = ServerApi('1'), 
                               connect = True)

    # Send a ping to confirm a successful connection
    try:
        mongodb_conn.admin.command('ping')
        mongodb_conn.server_info()

        print(':: Pinged your deployment. You successfully connected to MongoDB!')

        return mongodb_uri

    except Exception as e:
        print(e)

def fn_mongodb_write(mongodb_uri, rdd, sc, ss):
    
    # columns = ['abilities', 'base_experience', 'forms', 'game_indices', 'height', 'held_items', 'id',
    #            'is_default', 'location_area_encounters', 'moves', 'name', 'order', 'past_types', 'species', 
    #            'sprites', 'stats', 'types', 'weight']

    df = ss.createDataFrame(rdd, verifySchema = False)

    df.write\
      .format('mongodb')\
      .mode('overwrite')\
      .option('spark.mongodb.output.uri', mongodb_uri)\
      .option('database', database)\
      .option('collection', collection)\
      .save()

    return sc.stop(),\
           print(':: Data Inserted. Check Database!')

    # format('com.mongodb.spark.sql.DefaultSource')
    # .option('writeConcern.wTimeoutMS', 0)\
    # .option('writeConcern', 'majority')\
    # .option("spark.mongodb.output.uri", mongodb_conn)\
    # .option('database', database)\
    # .option('collection', collection)\

### EXEC ---------------------------------------
if __name__ == "__main__":

    ### GET DATA / MONGODB CONNECTION ---------------------------------------
    data = fn_request_pokemon_api()

    mongodb_uri = fn_mongodb_connection()

    ### SPARK SESSION / GET DATA ---------------------------------------
    sc, ss = fn_init_spark(mongodb_uri)

    rdd = sc.parallelize(data).collect()

    ### MONGODB WRITE DATA ---------------------------------------        
    fn_mongodb_write(mongodb_uri, rdd, sc, ss)
