import pymongo
from pymongo import MongoClient
from config import config
import csv
import time



HEADER = ['Region', 'Year', 'Mark']
while True:
    try:
        client = MongoClient(Config.MONGO_URI)
        db = client.cp_database
        collection = db.zno_res

        f = open('text.txt', 'w')
        start_time = time.time()
        f1 = open('D:\Desktop\Odata2019File.csv', 'r')
        r1 = csv.DictReader(csvfile1, delimiter=';')
        f2 = open('D:\Desktop\Odata2020File.csv', 'r')
        r2 = csv.DictReader(csvfile2, delimiter=';')
        
        zno_col = ['_id', 'zno_year', 'OUTID', 'Birth', 'SEXTYPENAME', 'REGNAME', 'AREANAME', 'TERNAME', 'REGTYPENAME',
                 'TerTypeName', 'ClassProfileNAME', 'ClassLangName', 'EONAME', 'EOTYPENAME', 'EORegName', 'EOAreaName',
                 'EOTerName', 'EOParent', 'UkrTest', 'UkrTestStatus', 'UkrBall100', 'UkrBall12', 'UkrBall',
                 'UkrAdaptScale', 'UkrPTName', 'UkrPTRegName', 'UkrPTAreaName', 'UkrPTTerName', 'histTest', 'HistLang',
                 'histTestStatus', 'histBall100', 'histBall12', 'histBall', 'histPTName', 'histPTRegName',
                 'histPTAreaName', 'histPTTerName', 'mathTest', 'mathLang', 'mathTestStatus', 'mathBall100',
                 'mathBall12', 'mathBall', 'mathPTName', 'mathPTRegName', 'mathPTAreaName', 'mathPTTerName', 'physTest',
                 'physLang', 'physTestStatus', 'physBall100', 'physBall12', 'physBall', 'physPTName', 'physPTRegName',
                 'physPTAreaName', 'physPTTerName', 'chemTest', 'chemLang', 'chemTestStatus', 'chemBall100',
                 'chemBall12', 'chemBall', 'chemPTName', 'chemPTRegName', 'chemPTAreaName', 'chemPTTerName', 'bioTest',
                 'bioLang', 'bioTestStatus', 'bioBall100', 'bioBall12', 'bioBall', 'bioPTName', 'bioPTRegName',
                 'bioPTAreaName', 'bioPTTerName', 'geoTest', 'geoLang', 'geoTestStatus', 'geoBall100', 'geoBall12',
                 'geoBall', 'geoPTName', 'geoPTRegName', 'geoPTAreaName', 'geoPTTerName', 'engTest', 'engTestStatus',
                 'engBall100', 'engBall12', 'engDPALevel', 'engBall', 'engPTName', 'engPTRegName', 'engPTAreaName',
                 'engPTTerName', 'fraTest', 'fraTestStatus', 'fraBall100', 'fraBall12', 'fraDPALevel', 'fraBall',
                 'fraPTName', 'fraPTRegName', 'fraPTAreaName', 'fraPTTerName', 'deuTest', 'deuTestStatus', 'deuBall100',
                 'deuBall12', 'deuDPALevel', 'deuBall', 'deuPTName', 'deuPTRegName', 'deuPTAreaName', 'deuPTTerName',
                 'spaTest', 'spaTestStatus', 'spaBall100', 'spaBall12', 'spaDPALevel', 'spaBall', 'spaPTName',
                 'spaPTRegName', 'spaPTAreaName', 'spaPTTerName']
          
         row_nums = [1, 18, 19, 20, 21, 29, 30, 31, 39, 40, 41, 49, 50, 51, 59, 60, 61, 69, 70, 71, 79, 80, 81, 88, 89, 91, 98, 99, 101, 108, 109, 111, 118, 119, 121]
          
         for elem in r1:
             row={}
             for col in zno_col:
                 row[col]=elem[col]
             db.zno.insert_one(row)
            
         for elem in r2:
             row={}
             for col in zno_col:
                 row[col]=elem[col]
             db.zno.insert_one(row)

        duration = time.time()-start_time
        with open('Duration.txt', 'w') as file:
	        	file.write(f'Duration of inserting data is {duration}')
            
            
        query = db.zno.aggregate([
            
                {
                     '$match': {'physTestStatus': 'Зараховано'}
                     },
                {
                     '$group': {
                            '_id': {
                                    'year': '$Year',
                                    'regname': '$REGNAME'},
                            'min': {
                                    '$min': '$physBall100'}
                      }
                },
                {
            '$sort' : { '_id.region': 1, '_id.year': 1 }
          }
        ])

        data = []
        for elem in query:
                row = [elem["_id"]["region"], elem["_id"]["year"], el["min"]]
                data.append(row)

        with open('Results.csv', 'w', newline='') as file:
                writer = csv.writer(file, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                writer.writerow(HEADER)
                for row in data:
                        writer.writerow(row)
        
        
        break
except Exception as err:
    print(err)
    time.sleep(5)



          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
