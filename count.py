import pymongo
client = pymongo.MongoClient('18.218.79.6', 27017)
client = pymongo.MongoClient('localhost', 27017)
database = client["EntityDB"]
count=0
collections=database.collection_names()
# collections.remove("system.indexes")
OFL_count=0
EXP_count=0
for coll in collections:
    collection = database[coll]

    OFL_CountryCount=collection.find({'_id': {'$regex': '^OFL_'}}).count()
    OFL_count+=OFL_CountryCount

    EXP_CountryCount = collection.find({'_id': {'$regex': '^EXP_'}}).count()
    EXP_count += EXP_CountryCount

    print "Country: {} -> Offshore Leaks count: {}, US Consolidated Screening List count:{}".format(collection.name,OFL_CountryCount,EXP_CountryCount)
print "\n\nTotal Offshore Leaks count:{}".format(OFL_count)
print "Total US Consolidated Screening List count:{}".format(EXP_count)
print "Total count:{}".format(OFL_count+EXP_count)
