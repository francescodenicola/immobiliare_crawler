import requests
from bs4 import BeautifulSoup
import pandas as pd
import csv
import json
from concurrent.futures import ThreadPoolExecutor, wait
import datetime
from time import sleep, time
from datetime import date
import os
from dotenv import load_dotenv, find_dotenv
import pyodbc
from models.models import Mapping
from utils.sqlite import create
import shutil

load_dotenv()


def connect(web_addr):
    resp = requests.get(web_addr)
    return BeautifulSoup(resp.content, "html.parser")


def get_links_immobiliare(web_addr):
    r = connect(web_addr)
    links = []
    for link in r.find_all('ul', {'class': 'annunci-list'}):
        for litag in link.find_all('li'):
            try:
                links.append(litag.a.get('href'))
            except BaseException:
                continue
    return links


def get_data_immobiliare(web_addr, path, name, origin):
    if not os.path.exists(path):
        os.mkdir(path)

    r = connect(web_addr)
    payload = r.find(id='js-hydration')
    json_object = json.loads(payload.contents[0])
    json_object.update(
        {'microzone': str(origin['zone_code']) + "" + str(origin['microzone_code'])})
    json_object.update({'url': str(web_addr)})

    #inserisco in fondo tabella immobiliare da frontend, per doublecheck e pulizia
    details = r.find("dl", {"class": "im-features__list"})
    dt = pd.DataFrame()
    cleaned_id_text = []
    for i in details.find_all('dt'):
        attribute = i.text
        attribute = attribute.replace("\n                    ","")
        attribute = attribute.replace("                ","")
        cleaned_id_text.append(attribute)
    cleaned_id__attrb_text = []
    for i in details.find_all('dd'):
        value = i.text
        value = value.replace("\n                    ","")
        value = value.replace("                ","")
        cleaned_id__attrb_text.append(value)
    dt['attribute'] = cleaned_id_text
    dt['value'] = cleaned_id__attrb_text
    df = dt.set_index('attribute').to_dict()
    json_object.update({'details': df})

    with open(path + "\\" + name + '.json', 'w') as outfile:
        json.dump(json_object, outfile)


def connectToSQL():
    DB_SERVER = os.environ.get("DB_SERVER")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    DB_NAME = os.environ.get("DB_NAME")
    cnxn = pyodbc.connect(
        'DRIVER={MySQL ODBC 8.0 ANSI Driver};SERVER=' +
        DB_SERVER +
        ';DATABASE=' +
        DB_NAME +
        ';UID=' +
        DB_USERNAME +
        ';PWD=' +
        DB_PASSWORD)
    return cnxn


def cleanDataImmobiliare(json_object):
    # prepare i dati prima dell'inserimento in sql
    country = json_object["listing"]["properties"][0]["location"]["nation"]["name"]
    region = json_object["listing"]["properties"][0]["location"]["region"]["name"]
    province = json_object["listing"]["properties"][0]["location"]["province"]["name"]
    city = json_object["listing"]["properties"][0]["location"]["city"]["name"]
    id = json_object["listing"]["id"]
    microzone = json_object["microzone"]
    url = json_object["url"]
    if json_object["listing"]["advertiser"]["agency"] != None:
        agency = json_object["listing"]["advertiser"]["agency"]["displayName"]
    else:
        agency = "Private"

    if json_object["listing"]["properties"][0]["location"]["address"] is None:
        address = ""
    else:
        if json_object["listing"]["properties"][0]["location"]["streetNumber"] is None:
            address = json_object["listing"]["properties"][0]["location"]["address"]
        else:
            address = json_object["listing"]["properties"][0]["location"]["address"] + " " + \
                json_object["listing"]["properties"][0]["location"]["streetNumber"].strip()

    mq = 0
    if ((str(json_object["listing"]["properties"][0]
             ["surfaceConstitution"]["totalMainSurface"]) is not None)):
        mq = str(json_object["listing"]["properties"][0]["surfaceConstitution"]
                 ["totalMainSurface"]).split("m\u00b2")[0].strip()
    if (str(json_object["listing"]["properties"][0]["surfaceValue"]) is not None) and (mq =="None"):
        mq = str(json_object["listing"]["properties"][0]
                 ["surfaceValue"]).split("m\u00b2")[0].strip()
    if str(json_object["listing"]["properties"][0]["surfaceValue"]) == "None":
        mq = "n.a."

    mq = mq.replace(",",".")

    range = 'tbl'

    try:
        if json_object["listing"]["properties"][0]["price"]["price"] is None:
            price = "0"
        else:
            price = json_object["listing"]["properties"][0]["price"]["price"]
    except e as Exception:
        price = "0"

    #print(json_object["details"])

    floor = "N.A."

    # must be surrounded by try / except
    if len(json_object["listing"]["properties"][0]
           ["surfaceConstitution"]["surfaceConstitutionElements"]) > 0:
        if str(json_object["listing"]["properties"][0]["surfaceConstitution"]
               ["surfaceConstitutionElements"][0]["floor"]).isdigit():
            floor = str(json_object["listing"]["properties"][0][
                        "surfaceConstitution"]["surfaceConstitutionElements"][0]["floor"])
            reason = "trovato floor"
        elif json_object["listing"]["properties"][0]["surfaceConstitution"]["surfaceConstitutionElements"][0]["floor"] == "Piano Rialzato":
            floor = "Piano Rialzato"
            reason = "rialzato"
        elif json_object["listing"]["properties"][0]["surfaceConstitution"]["surfaceConstitutionElements"][0]["floor"] == "Piano Terra":
            floor = "Piano Terra"
            reason = "terra"
        elif json_object["listing"]["properties"][0]["surfaceConstitution"]["surfaceConstitutionElements"][0]["floor"] == "Attico":
            floor = "Attico"
            reason = "attico"
    print("1 "+floor)
    if (floor == "N.A."):
        try:
            floor = json_object["details"]["piano"]
        except:
            pass
        
    if (floor == "N.A."):
        if json_object["listing"]["properties"][0]["description"].find("piano") != -1:
            txt = json_object["listing"]["properties"][0]["description"].split("piano")[0]
            reason = ("trovato da desc "+txt)
            if (txt.find("primo")>-1) or (txt.find("1\u00b0")>-1) or (txt.find("1°")>-1):
                floor = "1"
            elif (txt.find("secondo")>-1) or (txt.find("2\u00b0")>-1) or (txt.find("2°")>-1):
                floor = "2"
            elif (txt.find("terzo")>-1) or (txt.find("3\u00b0")>-1) or (txt.find("3°")>-1):
                floor = "3"
            elif (txt.find("quarto")>-1) or (txt.find("4\u00b0")>-1) or (txt.find("4°")>-1):
                floor = "4"
            elif (txt.find("quinto")>-1) or (txt.find("5\u00b0")>-1) or (txt.find("5°")>-1):
                floor = "5"
            elif (txt.find("sesto")>-1) or (txt.find("6\u00b0")>-1) or (txt.find("6°")>-1):
                floor = "6"
            elif (txt.find("settimo")>-1) or (txt.find("7\u00b0")>-1) or (txt.find("7°")>-1):
                floor = "7"
            elif (txt.find("ottavo")>-1) or (txt.find("8\u00b0")>-1) or (txt.find("8°")>-1):
                floor = "8"
            elif (txt.find("nono")>-1) or (txt.find("9\u00b0")>-1) or (txt.find("9°")>-1):
                floor = "9"
            elif (txt.find("dieci")>-1) or (txt.find("10\u00b0")>-1) or (txt.find("10°")>-1):
                floor = "10"
            elif (txt.find("attico")>-1):
                floor = "Attico"    
            elif (txt.find("piano terra")>-1):
                floor = "Piano Terra"
            elif (txt.find("piano rialzato")>-1):
                floor = "Piano Rialzato"
                print("ok")
        else:
            floor = "N.A."
            reason = 'flop'
            print(reason)
    print("2 "+floor)
    print(reason)
    if (json_object["listing"]["properties"][0]["description"].find("asta") != -1) or (json_object["listing"]["properties"][0]["description"].find("giudiziari") != -1):
        auction = 1
    else: auction = 0

    toilet = 0

    data = str(datetime.datetime.now().date())

    map = Mapping(
        Country=country.replace("'",r"\'"),
        Region=region.replace("'",r"\'"),
        Province=province.replace("'",r"\'"),
        City=city.replace("'",r"\'"),
        Microzone=microzone.replace("'",r"\'"),
        id=id,
        url=url,
        Agency=agency.replace("'",r"\'"),
        Address=address.replace("'",r"\'"),
        MQ=mq,
        Range=range,
        Price=price,
        Floor=str(floor),
        Auction=auction,
        Toilet=toilet,
        Date=data)

    #print(map)
    return map


def truncateSQLTable(connection, tableName):
    cursor = connection.cursor()
    cursor.execute("TRUNCATE TABLE " + tableName + " ;")
    connection.commit()
    connection.close()

def cleanAuctions(connection):
    cursor = connection.cursor()
    cursor.execute("DELETE FROM Mapping WHERE AUCTION <> 0;")
    deleted_row_count = cursor.rowcount
    connection.commit()
    connection.close()
    return deleted_row_count

def updateRangesMapping(connection):
    cursor = connection.cursor()
    cursor.execute("UPDATE Mapping A INNER JOIN Ranges B on CONVERT(CAST(A.MQ AS DECIMAL(10,6)),UNSIGNED INTEGER) >= B.MIN and CONVERT(CAST(A.MQ AS DECIMAL(10,6)),UNSIGNED INTEGER) <= B.MAX SET A.RANGE = B.RANGE ;")
    connection.commit()
    connection.close()

def updateAverages(connection):
    cursor = connection.cursor()
    cursor.execute("""
    UPDATE Averages
    INNER join 
    (SELECT concat(Mapping.MICROZONE,"_", Mapping.RANGE) AS MZ_RANGE,
    AVG(Mapping.PRICE) AS AVG_PREZZO,
    AVG(Mapping.PRICE / Mapping.MQ) AS AVG_PREZZO_MQ,
    COUNT(Mapping.ID) AS COUNT_MZ
    from Mapping
    GROUP BY Mapping.MICROZONE, Mapping.RANGE
    )Map 
    on Map.MZ_RANGE= Averages.MZ_RANGE
    SET Averages.AVG_PREZZO = Map.AVG_PREZZO,Averages.AVG_PREZZO_MQ = Map.AVG_PREZZO_MQ,Averages.PRICE_ABBATTUTO = Map.AVG_PREZZO*Averages.ABBATTIMENTO , Averages.PRICE_ABBATTUTO_MQ = Map.AVG_PREZZO_MQ*Averages.ABBATTIMENTO, CNT_MZ= COUNT_MZ;
    """)
    
    connection.commit()
    connection.close()

def insertAveragesIfOccurs(connection):
    cursor = connection.cursor()
    cursor.execute("""
    INSERT INTO Averages
    SELECT DISTINCT concat(Mapping.MICROZONE,"_", Mapping.RANGE),Mapping.MICROZONE, Mapping.RANGE, Ranges.ABBATTIMENTO, NULL,NULL, NULL,NULL,count(ID)
    from Mapping join Ranges on Mapping.RANGE = Ranges.RANGE
    WHERE concat(Mapping.MICROZONE,"_", Mapping.RANGE) NOT IN (SELECT DISTINCT MZ_RANGE FROM Averages)
    GROUP BY Mapping.MICROZONE, Mapping.RANGE, Ranges.ABBATTIMENTO;
    """)
    
    connection.commit()
    connection.close()

def insertToSQL(connection, rows):
    #print("cursor")
    cursor = connection.cursor()
    
    #print("end cursor")
    for row in rows:
        print(
        """INSERT INTO Mapping \
            VALUES \
            ( \
            '""" + row.Country + """', \
            '""" + row.Region + """', \
            '""" + row.Province + """', \
            '""" + row.City + """',\
            '""" + row.Microzone + """', \
            '""" + row.id + """', \
            '""" + row.url + """', \
            '""" + row.Agency + """', \
            '""" + row.Address + """', \
            '""" + str(row.MQ) + """', \
            '""" + row.Range + """', \
            '""" + str(row.Floor) + """', \
            """ + str(row.Auction) + """, \
            """ + str(row.Toilet) + """, \
            '""" + row.Date + """', \
            '""" + row.Price + """'); \
            """
            )
        #print("exec")
        #print(row)
        cursor.execute("""INSERT INTO Mapping \
        VALUES \
        ( \
        '""" + row.Country + """', \
        '""" + row.Region + """', \
        '""" + row.Province + """', \
        '""" + row.City + """',\
        '""" + row.Microzone + """', \
        '""" + row.id + """', \
        '""" + row.url + """', \
        '""" + row.Agency + """', \
        '""" + row.Address + """', \
        '""" + str(row.MQ) + """', \
        '""" + row.Range + """', \
        '""" + str(row.Floor) + """', \
        """ + str(row.Auction) + """, \
        """ + str(row.Toilet) + """, \
        '""" + row.Date + """', \
        '""" + row.Price + """'); \
        """
        )
    #print("pre-commit")
    connection.commit()
    #print("committed")

ROOT_DIR = os.path.dirname(os.path.abspath(__file__)) 
DIRECTORY = os.environ.get("DIRECTORY")

colnames = [
    'source',
    'country_code',
    'region_code',
    'province_code',
    'city_code',
    'zone_code',
    'zone_desc',
    'microzone_code',
    'url',
    'key']
data = pd.read_csv('torino.csv', names=colnames, sep=",")

urls = data.url.tolist()

i = 0
futures = []

start_time = time()
output_timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

path = ROOT_DIR+"\\IMMOBILIARE_test\\"

if os.path.exists(path):
    shutil.rmtree(path)
    
if not os.path.exists(path):
    os.mkdir(path)

with ThreadPoolExecutor() as executor:
    for index, row in data.iterrows():
        links = []
        i = i + 1
        path = ROOT_DIR +"\\IMMOBILIARE_test\\" + str(row['zone_code']) + \
            "" + str(row['microzone_code']) + "\\"
        print(path)
        url = row['url']
        links = get_links_immobiliare(url)
        print(str(i) + " lista da link " + url)
        j = 0
        for s in links:
            j = j + 1
            futures.append(
                executor.submit(get_data_immobiliare, s, path, str(j), row)
            )

end_time = time()
elapsed_time = end_time - start_time
print(f"Elapsed run time SCRAPING: {elapsed_time} seconds")

print("truncate")
start_time = time()
output_timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")


truncateSQLTable(connectToSQL(), 'Mapping')
truncateSQLTable(connectToSQL(), 'Averages')

end_time = time()
elapsed_time = end_time - start_time
print(f"Elapsed run time TRUNCATE: {elapsed_time} seconds")



rows = []
for subdir, dirs, files in os.walk(ROOT_DIR+"_test\\"):
    for filename in files:
        filepath = subdir + os.sep + filename
        if filepath.endswith(".json"):
            print(filepath)
            f = open(filepath)
            d = json.load(f)
            elem = cleanDataImmobiliare(d)
            if elem != None:
                rows.append(elem)


# print("insertion")
# start_time = time()
# output_timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
# insertToSQL(connectToSQL(), rows)
# end_time = time()
# elapsed_time = end_time - start_time
# print(f"Elapsed run time INSERTION: {elapsed_time} seconds")

# deleted_rows = cleanAuctions(connectToSQL())
# print("Sono state cancellate "+str(deleted_rows)+" aste")


# updateRangesMapping(connectToSQL())
# insertAveragesIfOccurs(connectToSQL())
# updateAverages(connectToSQL())

# conn = create.create_connection(ROOT_DIR+"\\utils\\sqlite\\immobiliare_crawler.db")



# if conn != None:
#     c = conn.cursor()
#     c.execute(
#         """
#         CREATE TABLE IF NOT EXISTS `mapping` (
#         `COUNTRY` varchar(50) NOT NULL,
#         `REGION` varchar(50) NOT NULL,
#         `PROVINCE` varchar(50) NOT NULL,
#         `CITY` varchar(50) NOT NULL,
#         `MICROZONE` varchar(10) NOT NULL,
#         `ID` varchar(50) NOT NULL,
#         `URL` varchar(200) NOT NULL,
#         `AGENCY` varchar(50) DEFAULT NULL,
#         `ADDRESS` varchar(100) NOT NULL,
#         `MQ` varchar(10) DEFAULT NULL,
#         `RANGE` varchar(50) NOT NULL,
#         `FLOOR` varchar(20) NOT NULL,
#         `AUCTION` int(1) NOT NULL,
#         `TOILET` int(1) NOT NULL,
#         `DATA` varchar(10) DEFAULT NULL,
#         `PRICE` varchar(20) NOT NULL
#         );
#         """
#     )

# conn.commit()
# conn.close()

