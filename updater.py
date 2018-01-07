#!/usr/bin/env python
import datetime
from StringIO import StringIO

import pandas as pd
import pymongo
import json
from geopy.geocoders import GoogleV3
from concurrent.futures import ThreadPoolExecutor
import requests
import zipfile


mongoClient = pymongo.MongoClient('localhost', 27017)
geolocator = GoogleV3(api_key='xxx')
ex = ThreadPoolExecutor(max_workers=200)


def get_lat_long(address):
    LatLongCollection = mongoClient['LatLongDB']['LatLongCollection']

    lat_long=LatLongCollection.find({'_id': address})
    if lat_long.count()>0:
        return  (lat_long.limit(1)[0]['latitude'],lat_long.limit(1)[0]['longitude'])
    try:
        lat_long = geolocator.geocode(address)
    except:
        try:
            lat_long=geolocator.geocode(address)
        except:
            return ("","")
    if lat_long:
        lat = lat_long.latitude
        long = lat_long.longitude
    else:
        lat = ""
        long = ""

    LatLongMap = {'_id': address, 'latitude': lat, 'longitude': long}
    LatLongCollection.insert(LatLongMap)
    return (lat,long)

def get_normalized_country_name(countryAddress):
    normalizedCountryNamesCollection = mongoClient['countryDB']['normalizedCountryNames']

    location=normalizedCountryNamesCollection.find({'_id': countryAddress})
    if location.count()>0:
        return  location.limit(1)[0]['normal_value']

    try:
        location = geolocator.geocode(countryAddress, language='en')
    except:
        try:
            location=geolocator.geocode(countryAddress, language='en')
        except:
            return "Not identified"

    if location != None and 'country' in location.raw['address_components'][0]['types']:
        longName=location.raw['address_components'][0]['long_name']
        countryCode=location.raw['address_components'][0]['short_name']
        countryMap = {'_id': countryAddress, 'normal_value': countryCode,'long_name':longName}
        normalizedCountryNamesCollection.insert(countryMap)
        return countryCode
    else:
        countryMap = {'_id': countryAddress, 'normal_value': 'Not identified','long_name':'Not identified'}
        normalizedCountryNamesCollection.insert(countryMap)
        return "Not identified"





def load_content(DB_Collection,id,row):
    oldRow=DB_Collection.find_and_modify(query={'_id': id}, update={'$set':row}, upsert=True, full_response=True)

    oldRow = oldRow.pop('value',None)
    if (oldRow == None):
        # print "New entry added"
        return
    oldRow.pop("_id", None)
    if(oldRow!=row):
        # shared_items = set(oldRow.items()) & set(row.items())
        # print len(shared_items)
        entry={'timestamp':datetime.datetime.now().isoformat(),'collection':DB_Collection.full_name,'value':oldRow}
        changesTimelineDB = mongoClient['changesTimelineDB']
        changesTimelineDB['changesTimeline'].insert(entry)


def loadOFL(oflDB, row):
    countries = []
    if row.get('countries'):
        countries = row.get('countries').split(";")

    addressess = []
    if row.get('address'):
        addressess = row.get('address').split(";")

    index = 0
    if (len(countries) == 0):
        oflDB_Collection = oflDB["Not identified"]
        id = "OFL_" + row['node_id']
        load_content(oflDB_Collection, id, row)

    while (index < len(countries)):
        country = countries[index]
        if len(countries) != len(addressess):
            address = row.get('address')
            if not address:
                address = ""
        else:
            address = addressess[index]
        row['address']=address
        lat=""
        long=""

        if address:
            lat,long=get_lat_long(address)

        row['latitude']=lat
        row['longitude']=long
        # print row
        # exit(0)

        index += 1

        country = country.encode('ascii', 'ignore')
        countryCode = get_normalized_country_name(country)
        collection_name = countryCode
        oflDB_Collection = oflDB[collection_name]
        id = "OFL_" + str(row['node_id'])
        load_content(oflDB_Collection, id, row)



def processOffshoreLeaks(offshoreLeaksFilePointer):
    oflDB = mongoClient['offshoreLeaksDB']
    data = pd.read_csv(offshoreLeaksFilePointer)
    data_json = json.loads(data.to_json(orient='records'))


    for row in data_json:
        ex.submit(loadOFL, oflDB,row)



if __name__ == "__main__":

    zipContent=requests.get("https://offshoreleaks-data.icij.org/offshoreleaks/csv/csv_offshore_leaks.2017-12-19.zip#_ga=2.164222911.1415007272.1515343932-1495118144.1515130262")
    fp = StringIO(zipContent.content)
    zipContent=None
    zfp=zipfile.ZipFile(fp,'r')
    OFLfp=zfp.open("offshore_leaks.nodes.entity.csv")
    processOffshoreLeaks(OFLfp)






