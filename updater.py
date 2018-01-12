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
import logging
from io import BytesIO
import sys

eventlet.monkey_patch()
logger = logging.getLogger()
logger.setLevel(logging.INFO)
sout_log_handler = logging.StreamHandler(sys.stdout)
file_log_handler = logging.FileHandler("updater.log")
sout_log_handler.setLevel(logging.DEBUG)
file_log_handler.setLevel(logging.DEBUG)
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
sout_log_handler.setFormatter(log_formatter)
file_log_handler.setFormatter(log_formatter)
logger.addHandler(sout_log_handler)
logger.addHandler(file_log_handler)

geolocator = GoogleV3(api_key='xxx')
ex = ThreadPoolExecutor(max_workers=200)

DB_IP = "localhost"
PORT = 27017
logger.info('Connecting to database at "{}" port "{}"'.format(DB_IP, PORT))
mongoClient = pymongo.MongoClient(DB_IP, PORT)


def get_lat_long(address):
    LatLongCollection = mongoClient['LatLongDB']['LatLongCollection']

    lat_long = LatLongCollection.find_one({'_id': address})
    if lat_long > 0:
        return (lat_long['latitude'], lat_long['longitude'])
    try:
        lat_long = geolocator.geocode(address, language='en')
    except:
        return ("", "")
        # Uncomment to retry 1 more time.
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
    logger.info('Cached lat long for {}'.format(address))

    return (lat, long)


def get_normalized_country_name(countryAddress):
    normalizedCountryNamesCollection = mongoClient['countryDB']['normalizedCountryNames']

    location = normalizedCountryNamesCollection.find_one({'_id': countryAddress})
    if location:
        return location['normal_value']

    try:
        location = geolocator.geocode(countryAddress, language='en')
    except:
        return "Not identified"
        # Uncomment to retry 1 more time.
        # try:
        #     # time.sleep(1)
        #     location=geolocator.geocode(countryAddress, language='en')
        # except Exception as e:
        #     print e
        #     return "Not identified"

    if location != None:
        for addrComponent in location.raw['address_components']:
            if 'country' in addrComponent['types']:
                longName = addrComponent['long_name']
                countryCode = addrComponent['short_name']
                countryMap = {'_id': countryAddress, 'normal_value': countryCode, 'long_name': longName}
                normalizedCountryNamesCollection.insert(countryMap)
                return countryCode

        countryMap = {'_id': countryAddress, 'normal_value': 'Not identified', 'long_name': 'Not identified'}
        normalizedCountryNamesCollection.insert(countryMap)
        return "Not identified"
    else:
        countryMap = {'_id': countryAddress, 'normal_value': 'Not identified', 'long_name': 'Not identified'}
        normalizedCountryNamesCollection.insert(countryMap)
        return "Not identified"
    logger.info('Cached Country code for {}'.format(countryAddress))


def load_content(DB, Country, id, row):
    Collection = DB[Country]

    if Country != "Not identified":
        DB["Not identified"].find_one_and_delete({'_id': id})

    oldRow = Collection.find_and_modify(query={'_id': id}, update={'$set': row}, upsert=True, full_response=True)

    oldRow = oldRow.pop('value', None)
    if (oldRow == None):
        logger.info('New document added to EntityDB.')
        return
    oldRow.pop("_id", None)
    if sorted(oldRow.items()) != sorted(row.items()):
        logger.info('Entry modified in EntityDB, changes recorded in changesTimelineDB.')
        entry = {'timestamp': datetime.datetime.now().isoformat(), 'collection': Collection.full_name,
                 'old_value': oldRow, 'new_value': row}
        changesTimelineDB = mongoClient['changesTimelineDB']
        changesTimelineDB['changesTimeline'].insert(entry)
    else:
        logger.info('Document unchanged')


def loadOFL(EntityDB, row):
    countries = []
    if row.get('countries'):
        countries = row.get('countries').split(";")

    addresses = []
    all_address = ""
    if row.get('address'):
        all_address = row.get('address')
        addresses = row.get('address').split(";")

    row['all_addresses'] = all_address

    index = 0
    if (len(countries) == 0):
        id = "OFL_" + row['node_id']
        load_content(EntityDB, "Not identified", id, row)
        return

    while (index < len(countries)):
        country = countries[index]
        if len(countries) != len(addresses):
            address = row.get('address')
            if not address:
                address = ""
        else:
            address = addresses[index]
        row['address'] = address

        if address:
            lat, long = get_lat_long(address)
            row['latitude'] = lat
            row['longitude'] = long
        else:
            row['latitude'] = ""
            row['longitude'] = ""

        index += 1

        countryCode = get_normalized_country_name(country)
        id = u''.join(filter(None, ("OFL_", str(row.get('node_id')), row.get('address'), country)))
        load_content(EntityDB, countryCode, id, row)


def processOffshoreLeaks(offshoreLeaksFilePointer):
    EntityDB = mongoClient['EntityDB']
    data = pd.read_csv(offshoreLeaksFilePointer)
    data_json = json.loads(data.to_json(orient='records'))

    for row in data_json:
        ex.submit(loadOFL, EntityDB, row)
        # loadOFL(EntityDB,row)
    logger.info('OFFSHORE LEAKS Jobs submitted!!')


def loadEXP(EntityDB, row):
    addresses = []
    if row.get('addresses'):
        addresses = row.get('addresses').split(";")
        row['all_addresses'] = row.pop('addresses')

    index = 0
    if (len(addresses) == 0):
        Country = "Not identified"
        row["address"] = ""
        id = u''.join(filter(None, (
            "EXP_", row.get("name"), row.get('source_information_url'), str(row.get('entity_number')),
            row.get('address'),
            row.get('federal_register_notice'))))
        load_content(EntityDB, Country, id, row)
        return

    while (index < len(addresses)):
        address = addresses[index]
        row["address"] = address
        id = u''.join(filter(None, (
            "EXP_", row.get("name"), row.get('source_information_url'), str(row.get('entity_number')),
            row.get('address'),
            row.get('federal_register_notice'))))

        countryCode = get_normalized_country_name(address)

        if countryCode:
            lat, long = get_lat_long(address)
            row['latitude'] = lat
            row['longitude'] = long
        else:
            row['latitude'] = ""
            row['longitude'] = ""

        index += 1

        load_content(EntityDB, countryCode, id, row)


def processEXP_GOV_ConsolidatedScrnList(EXP_GOV_ConsolidatedScrnList):
    EntityDB = mongoClient['EntityDB']
    data = pd.read_csv(BytesIO(EXP_GOV_ConsolidatedScrnList.decode('utf8', 'ignore').encode('utf-8')))
    data_json = json.loads(data.to_json(orient='records'))

    for row in data_json:
        # loadEXP(EntityDB, row)
        ex.submit(loadEXP, EntityDB, row)
    logger.info('US Consolidated Screening List Jobs submitted!!')


try:
    with eventlet.Timeout(30):
        logger.info('Processing OFFSHORE LEAKS')
        IPLINK = "https://offshoreleaks-data.icij.org/offshoreleaks/csv/csv_offshore_leaks.2017-12-19.zip#_ga=2.164222911.1415007272.1515343932-1495118144.1515130262"
        logger.info('Downloading {}'.format(IPLINK))
        OFLzipContent = requests.get(IPLINK)
    fp = StringIO(OFLzipContent.content)
    zipContent = None
    zfp = zipfile.ZipFile(fp, 'r')
    logger.info('Unzipping contents')
    OFLfp = zfp.open("offshore_leaks.nodes.entity.csv")
    processOffshoreLeaks(OFLfp)
except Exception as e:
    logger.info('OFFSHORE LEAKS link Timed out')
    logger.info(e)

try:
    with eventlet.Timeout(30):
        logger.info('Processing US Consolidated Screening List')
        IPLINK = "https://api.trade.gov/consolidated_screening_list/search.csv?api_key=OHZYuksFHSFao8jDXTkfiypO"
        logger.info('Downloading {}'.format(IPLINK))
        EXP_GOV_ConsolidatedScrnList = requests.get(IPLINK).content
    processEXP_GOV_ConsolidatedScrnList(EXP_GOV_ConsolidatedScrnList)
except Exception as e:
    logger.info('US Consolidated Screening List link Timed out')
    logger.info(e)

ex.shutdown()
logger.info('DONE!!')
