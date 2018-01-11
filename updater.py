#!/usr/bin/env python
import datetime
import time
from StringIO import StringIO
import eventlet

import pandas as pd
import pymongo
import json
from geopy.geocoders import GoogleV3
from concurrent.futures import ThreadPoolExecutor
import requests
import zipfile

from io import BytesIO

mongoClient = pymongo.MongoClient('localhost', 27017)
geolocator = GoogleV3(api_key='AIzaSyCV2dxrI4RItaKqOoenXr5osbEqazE6lYs')
ex = ThreadPoolExecutor(max_workers=200)
eventlet.monkey_patch()

def get_lat_long(address):
    LatLongCollection = mongoClient['LatLongDB']['LatLongCollection']

    lat_long=LatLongCollection.find_one({'_id': address})
    if lat_long>0:
        return  (lat_long['latitude'],lat_long['longitude'])
    try:
        lat_long = geolocator.geocode(address,language='en')
    except:
        return ("", "")
        #Uncomment to retry 1 more time.
        # try:
        #     # time.sleep(1)
        #     lat_long=geolocator.geocode(address, language='en')
        # except Exception as e:
        #     print e
        #     return ("","")
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

    location=normalizedCountryNamesCollection.find_one({'_id': countryAddress})
    if location:
        return location['normal_value']

    try:
        location = geolocator.geocode(countryAddress, language='en')
    except:
        try:
            # time.sleep(1)
            location=geolocator.geocode(countryAddress, language='en')
        except Exception as e:
            print e
            return "Not identified"

    if location != None:
        for addrComponent in location.raw['address_components']:
            if 'country' in addrComponent['types']:
                longName=addrComponent['long_name']
                countryCode=addrComponent['short_name']
                countryMap = {'_id': countryAddress, 'normal_value': countryCode,'long_name':longName}
                normalizedCountryNamesCollection.insert(countryMap)
                return countryCode
            else:
                countryMap = {'_id': countryAddress, 'normal_value': 'Not identified', 'long_name': 'Not identified'}
                normalizedCountryNamesCollection.insert(countryMap)
                return "Not identified"
    else:
        countryMap = {'_id': countryAddress, 'normal_value': 'Not identified','long_name':'Not identified'}
        normalizedCountryNamesCollection.insert(countryMap)
        return "Not identified"





def load_content(DB,Country,id,row):
    Collection=DB[Country]

    if Country!="Not identified":
        DB["Not identified"].find_one_and_delete({'_id':id})

    oldRow=Collection.find_and_modify(query={'_id': id}, update={'$set':row}, upsert=True, full_response=True)

    oldRow = oldRow.pop('value',None)
    print "yay"
    if (oldRow == None):
        return
    oldRow.pop("_id", None)
    if sorted(oldRow.items())!=sorted(row.items()):
        entry={'timestamp':datetime.datetime.now().isoformat(),'collection':Collection.full_name,'old_value':oldRow,'new_value':row}
        changesTimelineDB = mongoClient['changesTimelineDB']
        changesTimelineDB['changesTimeline'].insert(entry)


def loadOFL(EntityDB, row):
    countries = []
    if row.get('countries'):
        countries = row.get('countries').split(";")

    addressess = []
    all_address=""
    if row.get('address'):
        all_address=row.get('address')
        addressess = row.get('address').split(";")

    row['addressess'] = all_address

    index = 0
    if (len(countries) == 0):
        id = "OFL_" + row['node_id']
        load_content(EntityDB,"Not identified", id, row)

    while (index < len(countries)):
        country = countries[index]
        if len(countries) != len(addressess):
            address = row.get('address')
            if not address:
                address = ""
        else:
            address = addressess[index]
        row['address']=address


        if address:
            lat,long=get_lat_long(address)
            row['latitude']=lat
            row['longitude']=long
        else:
            row['latitude'] = ""
            row['longitude'] = ""

        index += 1

        countryCode = get_normalized_country_name(country)
        id = u''.join(filter(None,("OFL_",str(row.get('node_id')),row.get('address'),country)))
        load_content(EntityDB,countryCode,id, row)



def processOffshoreLeaks(offshoreLeaksFilePointer):
    EntityDB = mongoClient['EntityDB']
    data = pd.read_csv(offshoreLeaksFilePointer)
    data_json = json.loads(data.to_json(orient='records'))


    for row in data_json:
        ex.submit(loadOFL, EntityDB,row)
        # loadOFL(EntityDB,row)

def loadEXP(EntityDB, row):

    addressess = []
    if row.get('addresses'):
        addressess = row.get('addresses').split(";")

    index = 0
    if (len(addressess) == 0):
        Country="Not identified"
        row["address"] = ""
        id=u''.join(filter(None,("EXP_",row.get("name"),row.get('source_information_url'),str(row.get('entity_number')),row.get('address'),row.get('federal_register_notice'))))
        load_content(EntityDB,Country,id, row)

    while (index < len(addressess)):
        address = addressess[index]
        row["address"] = address
        id=u''.join(filter(None,("EXP_",row.get("name"),row.get('source_information_url'),str(row.get('entity_number')),row.get('address'),row.get('federal_register_notice'))))


        countryCode = get_normalized_country_name(address)

        if countryCode:
            lat, long = get_lat_long(address)
            row['latitude'] = lat
            row['longitude'] = long
        else:
            row['latitude'] = ""
            row['longitude'] = ""


        index += 1

        load_content(EntityDB,countryCode,id,row)



def processEXP_GOV_ConsolidatedScrnList(EXP_GOV_ConsolidatedScrnList):
    EntityDB = mongoClient['EntityDB']
    data=pd.read_csv(BytesIO(EXP_GOV_ConsolidatedScrnList.decode('utf8', 'ignore').encode('utf-8')))
    data_json = json.loads(data.to_json(orient='records'))

    for row in data_json:
        # loadEXP(EntityDB, row)
        ex.submit(loadEXP, EntityDB, row)


if __name__ == "__main__":
    try:
        with eventlet.Timeout(10):
            OFLzipContent=requests.get("https://offshoreleaks-data.icij.org/offshoreleaks/csv/csv_offshore_leaks.2017-12-19.zip#_ga=2.164222911.1415007272.1515343932-1495118144.1515130262")
        fp = StringIO(OFLzipContent.content)
        zipContent=None
        zfp=zipfile.ZipFile(fp,'r')
        OFLfp=zfp.open("offshore_leaks.nodes.entity.csv")
        processOffshoreLeaks(OFLfp)
    except Exception as e:
        print e
        pass
    try:
        with eventlet.Timeout(10):
            EXP_GOV_ConsolidatedScrnList=requests.get("https://api.trade.gov/consolidated_screening_list/search.csv?api_key=OHZYuksFHSFao8jDXTkfiypO").content
        processEXP_GOV_ConsolidatedScrnList(EXP_GOV_ConsolidatedScrnList)
    except Exception as e:
        print e
        pass



