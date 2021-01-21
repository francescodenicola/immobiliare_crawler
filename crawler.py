import requests
from bs4 import BeautifulSoup
import pandas as pd
import csv
import json
from concurrent.futures import ThreadPoolExecutor, wait, as_completed   
import datetime
from time import sleep, time
from datetime import date
import os
from dotenv import load_dotenv, find_dotenv
import pyodbc
from models.models import Mapping
from utils.sqlite import create
import shutil
import string
import random
import logging


load_dotenv()
formatLOG = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

def LOG_insert(file, format, text, level):
    infoLog = logging.FileHandler(file)
    infoLog.setFormatter(format)
    logger = logging.getLogger(file)
    logger.setLevel(level)
    
    if not logger.handlers:
        logger.addHandler(infoLog)
        if (level == logging.INFO):
            logger.info(text+"\n")
        if (level == logging.ERROR):
            logger.error(text+"\n")
        if (level == logging.WARNING):
            logger.warning(text+"\n")
    
    infoLog.close()
    logger.removeHandler(infoLog)
    
    return


def connect(web_addr):
    resp = requests.get(web_addr)
    return BeautifulSoup(resp.content, "html.parser")


def get_links_immobiliare(web_addr, page=1):
    r = connect(web_addr)
    links = []
    count = 0
    for link in r.find_all('ul', {'class': 'annunci-list'}):
        for litag in link.find_all('li'):
            try:
                links.append(litag.a.get('href'))
                count += 1
            except BaseException:
                if count == 25:
                    page = page + 1
                    count = count + 1
                    link_next = get_links_immobiliare(web_addr+"&pag="+str(page),page)
                    links=links+link_next
                continue
    return links


def get_data_immobiliare(web_addr, path, name, origin):
    try:
        if not os.path.exists(path):
            os.mkdir(path)
    except:
        pass

    r = connect(web_addr)
    payload = r.find(id='js-hydration')
    json_object = json.loads(payload.contents[0])
    json_object.update(
        {'microzone': str(origin['zone_code']) + "" + str(origin['microzone_code'])})
    json_object.update({'url': str(web_addr)})

    try:
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
    except Exception as e: 
        LOG_insert("file.log", formatLOG , "details wrong or missing", logging.WARNING)
        print(e) 

    try:
        # if not os.path.exists(path + "\\" + name + '.json'):
        if not os.path.exists(os.path.join(path,name + '.json')):
            # with open(path + "\\" + name + '.json', 'w') as outfile:
            with open(os.path.join(path,name + '.json'), 'w') as outfile:
                json.dump(json_object, outfile)
        else:
            # with open(path + "\\" + ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(5)) + '.json', 'w') as outfile:
            with open(os.path.join(path, ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(5))) + '.json', 'w') as outfile:
                json.dump(json_object, outfile)
    except Exception as e: 
        LOG_insert("file.log", formatLOG , "exception on file name creation", logging.WARNING)
        print(e)
        # with open(path + "\\" + ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(5)) + '.json', 'w') as outfile:
        with open(os.path.join(path, ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(5))) + '.json', 'w') as outfile:
            json.dump(json_object, outfile)
    return name

def connectToSQL(type):
    DB_SERVER = os.environ.get("DB_SERVER")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    DB_NAME = os.environ.get("DB_NAME")
    DB_PORT = os.environ.get("DB_PORT")
    if type == 'mysql':
        cnxn = pyodbc.connect(
            'DRIVER={MySQL ODBC 8.0 ANSI Driver};SERVER=' +
            DB_SERVER +
            ';DATABASE=' +
            DB_NAME +
            ';UID=' +
            DB_USERNAME +
            ';PWD=' +
            DB_PASSWORD)
    else:
        cnxn = pyodbc.connect(
            'Driver={ODBC Driver 17 for SQL Server};Server=tcp:'+
            DB_SERVER + "," + DB_PORT + ';DATABASE=' +
            DB_NAME +
            ';UID=' +
            DB_USERNAME +
            ';PWD=' +
            DB_PASSWORD)
    return cnxn

def connectToSQLite():
    # conn = create.create_connection(ROOT_DIR+"\\utils\\sqlite\\immobiliare_crawler.db")
    ROOT_DIR = os.path.dirname(os.path.abspath(__file__)) 
    conn = create.create_connection(os.path.join(ROOT_DIR,"utils","sqlite","immobiliare_crawler.db"))
    return conn

def cleanDataImmobiliare(json_object):
     # prepare i dati prima dell'inserimento in sql
    try:
        country = json_object["listing"]["properties"][0]["location"]["nation"]["name"]
    except e as Exception:
        print(e)
        LOG_insert("file.log", formatLOG , "missing country "+ e, logging.WARNING)
        country = 'Italia'

    try:
        region = json_object["listing"]["properties"][0]["location"]["region"]["name"]
    except e as Exception:
        print(e)
        LOG_insert("file.log", formatLOG , "missing region "+ e, logging.WARNING)
        region = 'Piemonte'
    try:
        province = json_object["listing"]["properties"][0]["location"]["province"]["name"]
        except e as Exception:
        print(e)
        LOG_insert("file.log", formatLOG , "missing province "+ e, logging.WARNING)
        region = 'Torino'
    try:    
        city = json_object["listing"]["properties"][0]["location"]["city"]["name"]
    except e as Exception:
        print(e)
        LOG_insert("file.log", formatLOG , "missing city "+ e, logging.WARNING)
        region = 'Torino'

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
    
    try:
        if ((str(json_object["listing"]["properties"][0]["surfaceConstitution"]["totalMainSurface"]) is not None)):
            mq = str(json_object["listing"]["properties"][0]["surfaceConstitution"]["totalMainSurface"]).split("m\u00b2")[0].strip()
        if (str(json_object["listing"]["properties"][0]["surfaceValue"]) is not None) and (mq =="None"):
            mq = str(json_object["listing"]["properties"][0]["surfaceValue"]).split("m\u00b2")[0].strip()
        if str(json_object["listing"]["properties"][0]["surfaceValue"]) == "None":
            mq = "n.a."
    except:
        mq = "N.A."
        pass

    if mq != "N.A.":
        mq = mq.replace(",",".")
        if mq.find(".")!=-1:
            mq_tmp = mq.split(".")
            mq = mq_tmp[0]
        

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

    try:
        # must be surrounded by try / except
        if len(json_object["listing"]["properties"][0]
            ["surfaceConstitution"]["surfaceConstitutionElements"]) > 0:
            if str(json_object["listing"]["properties"][0]["surfaceConstitution"]
                ["surfaceConstitutionElements"][0]["floor"]).isdigit():
                floor = str(json_object["listing"]["properties"][0][
                            "surfaceConstitution"]["surfaceConstitutionElements"][0]["floor"])
            elif json_object["listing"]["properties"][0]["surfaceConstitution"]["surfaceConstitutionElements"][0]["floor"] == "Piano Rialzato":
                floor = "Piano Rialzato"
            elif json_object["listing"]["properties"][0]["surfaceConstitution"]["surfaceConstitutionElements"][0]["floor"] == "Piano Terra":
                floor = "Piano Terra"
            elif json_object["listing"]["properties"][0]["surfaceConstitution"]["surfaceConstitutionElements"][0]["floor"] == "Attico":
                floor = "Attico"
        if (floor == "N.A."):
            try:
                floor = str(json_object["details"]["value"]["piano"])
            except:
                pass
            
        if (floor == "N.A."):
            if json_object["listing"]["properties"][0]["description"].find("piano") != -1:
                txt = json_object["listing"]["properties"][0]["description"].split("piano")[0]
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

            else:
                floor = "N.A."
    except:
        floor = "N.A."
        pass

    floor = floor.replace("° piano, con ascensore","")
    floor = floor.replace("° piano","")

    auction = 0
    #flag per cancellare nuove costruzioni
    if(json_object["listing"]["properties"][0]["condition"]!=None):
        if json_object["listing"]["properties"][0]["condition"]["id"]== "1":
            auction = 1

    #flag per cancellare aste
    if (auction == 0) and \
        ((json_object["listing"]["properties"][0]["description"].find(" asta ") != -1) or \
        (json_object["listing"]["properties"][0]["description"].find(" giudiziari") != -1)) or \
        'Tribunale' in agency:
        auction = 1

    #toilet (da migliorare)
    if "bagni" in (json_object["trovakasa"]):
        toilet = int(json_object["trovakasa"]["bagni"])
    else:
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

    return map


def truncateSQLTable(connection, tableName,type):
    cursor = connection.cursor()
    if type == 'mysql':
        cursor.execute("TRUNCATE TABLE " + tableName + " ;")
    elif type == 'sqlite':
        cursor.execute("DELETE FROM " + tableName + " ;")
    connection.commit()
    connection.close()

def cleanAuctions(connection):
    cursor = connection.cursor()
    cursor.execute("DELETE FROM Mapping WHERE AUCTION <> 0;")
    deleted_row_count = cursor.rowcount
    connection.commit()
    connection.close()
    return deleted_row_count

def cleanZeroPrice(connection):
    cursor = connection.cursor()
    cursor.execute("DELETE FROM Mapping WHERE PRICE = 0;")
    deleted_row_count = cursor.rowcount
    connection.commit()
    connection.close()
    return deleted_row_count

def cleanZeroMQ(connection):
    cursor = connection.cursor()
    cursor.execute("DELETE FROM Mapping WHERE MQ = '';")
    deleted_row_count = cursor.rowcount
    connection.commit()
    connection.close()
    return deleted_row_count

def cleanOutOfRange(connection,type):
    cursor = connection.cursor()
    if type == 'mysql_old':
        cursor.execute(
            """
            delete from Mapping 
            where Mapping.ID in(
            select del.ID from(
            select Mapping.id,case when Averages.RANGE = '0_50' OR Averages.RANGE = '51_70' then (Mapping.Price - Averages.PRICE_ABBATTUTO) / Averages.PRICE_ABBATTUTO
                    ELSE
                    ((Mapping.PRICE / Mapping.MQ) - Averages.PRICE_ABBATTUTO_MQ) / Averages.PRICE_ABBATTUTO_MQ
                    END
                    as ABBATTIMENTO
                    from Mapping INNER JOIN  Averages ON 
                    concat(Mapping.MICROZONE, "_", Mapping.RANGE)  = Averages.MZ_RANGE
            )del where ABBATTIMENTO > 1
            )
            """
        )
    elif type == 'mysql':
        cursor.execute(
            """
            delete from Mapping 
            where Mapping.ID in(
            select del.ID from(
            select Mapping.id,case when Averages.RANGE = '0_50' OR Averages.RANGE = '51_70' then (Mapping.Price - Averages.PRICE_ABBATTUTO) / Averages.PRICE_ABBATTUTO
                    ELSE
                    ((try_cast(Mapping.PRICE as int) / try_cast(Mapping.MQ as int) ) - try_cast(Averages.PRICE_ABBATTUTO_MQ as int)) / try_cast(Averages.PRICE_ABBATTUTO_MQ as int)
                    END
                    as ABBATTIMENTO
                    from Mapping INNER JOIN  Averages ON 
                    concat(Mapping.MICROZONE, '_', Mapping.RANGE)  = Averages.MZ_RANGE
            )del where ABBATTIMENTO > 1
 
            )
            """
        )
    elif type == 'sqlite':
        cursor.execute(
            """
            delete from Mapping 
            where Mapping.ID in(
            select del.ID from(
            select Mapping.id,case when Averages.RANGE = '0_50' OR Averages.RANGE = '51_70' then (Mapping.Price - Averages.PRICE_ABBATTUTO) / Averages.PRICE_ABBATTUTO
            ELSE
            ((Mapping.PRICE / Mapping.MQ) - Averages.PRICE_ABBATTUTO_MQ) / Averages.PRICE_ABBATTUTO_MQ
            END
            as ABBATTIMENTO
            from Mapping INNER JOIN  Averages ON 
            Mapping.MICROZONE || "_" || Mapping.RANGE  = Averages.MZ_RANGE
            )del where ABBATTIMENTO > 1
            )
            """
        )

def updateRangesMapping(connection,type):
    cursor = connection.cursor()
    if type == "mysql_old":
        cursor.execute("UPDATE Mapping INNER JOIN Ranges on CONVERT(CAST(Mapping.MQ AS DECIMAL(10,6)),UNSIGNED INTEGER) >= Ranges.MIN and CONVERT(CAST(Mapping.MQ AS DECIMAL(10,6)),UNSIGNED INTEGER) <= Ranges.MAX SET Mapping.RANGE = Ranges.RANGE ;")
    elif type == "mysql":
        cursor.execute("UPDATE Mapping SET Mapping.RANGE = Ranges.RANGE from mapping INNER JOIN Ranges on CONVERT(INTEGER,TRY_CAST(Mapping.MQ AS DECIMAL(10,6))) >= Ranges.MIN and CONVERT(INTEGER,TRY_CAST(Mapping.MQ AS DECIMAL(10,6))) <= Ranges.MAX ")
    elif type == "sqlite":
        cursor.execute(
            """
            UPDATE Mapping SET RANGE = 
            (SELECT RANGE 
            FROM Ranges 
            WHERE (CAST(CAST(Mapping.MQ AS DECIMAL(10,6)) AS INTEGER) >= Ranges.MIN) and (CAST(CAST(Mapping.MQ AS DECIMAL(10,6)) AS INTEGER) <= Ranges.MAX))
            WHERE EXISTS 
            (SELECT * 
            FROM Ranges 
            WHERE (CAST(CAST(Mapping.MQ AS DECIMAL(10,6)) AS INTEGER) >= Ranges.MIN) and (CAST(CAST(Mapping.MQ AS DECIMAL(10,6)) AS INTEGER) <= Ranges.MAX))          
            """)
    connection.commit()
    connection.close()

def updateAverages(connection,type):
    cursor = connection.cursor()
    if type == 'mysql_old':
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
    elif type == 'mysql':
        cursor.execute("""
        UPDATE Averages
        SET Averages.AVG_PREZZO = Map.AVG_PREZZO,Averages.AVG_PREZZO_MQ = Map.AVG_PREZZO_MQ,Averages.PRICE_ABBATTUTO = Map.AVG_PREZZO*Averages.ABBATTIMENTO , Averages.PRICE_ABBATTUTO_MQ = Map.AVG_PREZZO_MQ*Averages.ABBATTIMENTO, CNT_MZ= COUNT_MZ
        FROM Averages
        INNER join 
        (SELECT concat(Mapping.MICROZONE,'_', Mapping.RANGE) AS MZ_RANGE,
        AVG(try_cast(try_cast(Mapping.PRICE as numeric) AS int)) AS AVG_PREZZO,
        AVG(try_cast(try_cast(Mapping.PRICE as numeric) as int) / try_cast(try_cast(Mapping.MQ as numeric) as int)) AS AVG_PREZZO_MQ,
        COUNT(Mapping.ID) AS COUNT_MZ
        from Mapping
        GROUP BY Mapping.MICROZONE, Mapping.RANGE
        )Map 
        on Map.MZ_RANGE= Averages.MZ_RANGE  
        """)
    elif type == 'sqlite':
        cursor.execute(
        """
        UPDATE Averages
        SET AVG_PREZZO = (
	    SELECT
        AVG(Mapping.PRICE)
	    FROM Mapping
	    WHERE Averages.MZ_RANGE = Mapping.MICROZONE || "_" || Mapping.RANGE),
        AVG_PREZZO_MQ = (
	    SELECT
        AVG(Mapping.PRICE / Mapping.MQ)
	    FROM Mapping
	    WHERE Averages.MZ_RANGE = Mapping.MICROZONE || "_" || Mapping.RANGE),
        PRICE_ABBATTUTO = (
	    SELECT
        AVG(Mapping.PRICE)
	    FROM Mapping
	    WHERE Averages.MZ_RANGE = Mapping.MICROZONE || "_" || Mapping.RANGE)*Averages.ABBATTIMENTO,
        PRICE_ABBATTUTO_MQ = (
	    SELECT
        AVG(Mapping.PRICE / Mapping.MQ)
	    FROM Mapping
	    WHERE Averages.MZ_RANGE = Mapping.MICROZONE || "_" || Mapping.RANGE)*Averages.ABBATTIMENTO,
        CNT_MZ = (
	    SELECT
	    COUNT(Mapping.ID)
	    FROM Mapping
	    WHERE Averages.MZ_RANGE = Mapping.MICROZONE || "_" || Mapping.RANGE)
        WHERE EXISTS (select * from Mapping where Averages.MZ_RANGE = Mapping.MICROZONE || "_" || Mapping.RANGE)
        """    
        )
    connection.commit()
    connection.close()

def updateOpportunitiesValues(connection,type):
    cursor = connection.cursor()
    if type == "mysql_old":
        cursor.execute("""
        UPDATE Opportunity
        JOIN
        (select Mapping.ID as ID,Mapping.PRICE as P, Mapping.PRICE/Mapping.MQ as PMQ, case when Averages.RANGE = '0_50' OR Averages.RANGE = '51_70' then (Mapping.Price - Averages.PRICE_ABBATTUTO) / Averages.PRICE_ABBATTUTO
                ELSE
                ((Mapping.PRICE / Mapping.MQ) - Averages.PRICE_ABBATTUTO_MQ) / Averages.PRICE_ABBATTUTO_MQ
                END
                as ABBATTIMENTO
                from Mapping JOIN Opportunity ON Mapping.ID = Opportunity.ID INNER JOIN  Averages ON 
                concat(Mapping.MICROZONE, "_", Mapping.RANGE)  = Averages.MZ_RANGE 
        )ABB
        on ID = ABB.ID
        SET Opportunity.PRICE = ABB.P,
        Opportunity.PRICE_MQ = ABB.PMQ,
        Opportunity.SCOSTAMENTO = ABB.ABBATTIMENTO;
        """)
    elif type == "mysql":
        """
        UPDATE Opportunity
        SET Opportunity.PRICE = ABB.P,
        Opportunity.PRICE_MQ = ABB.PMQ,
        Opportunity.SCOSTAMENTO = ABB.ABBATTIMENTO
        FROM Opportunity
        JOIN
        (select Mapping.ID as ID,try_cast(try_cast(Mapping.Price as numeric) as int) as P, Mapping.PRICE/try_cast(try_cast(Mapping.MQ as numeric) as int)  as PMQ, case when Averages.RANGE = '0_50' OR Averages.RANGE = '51_70' then (try_cast(try_cast(Mapping.Price as numeric) as float) - try_cast(try_cast(Averages.PRICE_ABBATTUTO as numeric) as float) ) / try_cast(try_cast(Averages.PRICE_ABBATTUTO as numeric) as float) 
                ELSE
                ((try_cast(try_cast(Mapping.Price as numeric) as float) / try_cast(try_cast(Mapping.MQ as numeric) as float) ) - try_cast(try_cast(Averages.PRICE_ABBATTUTO_MQ as numeric) as int)) / try_cast(try_cast(Averages.PRICE_ABBATTUTO_MQ as numeric) as float)
                END
                as ABBATTIMENTO
                from Mapping JOIN Opportunity ON Mapping.ID = Opportunity.ID INNER JOIN  Averages ON 
                concat(Mapping.MICROZONE, '_', Mapping.RANGE)  = Averages.MZ_RANGE  
        )ABB
        on ID = ABB.ID
        """
    elif type == "sqlite":
        cursor.execute(
            """
            UPDATE Opportunity
            SET 
            DATA = (SELECT Mapping.DATA FROM Mapping WHERE Opportunity.ID = Mapping.ID),
            PRICE = (SELECT Mapping.PRICE FROM Mapping WHERE Opportunity.ID = Mapping.ID),
            PRICE_MQ = (SELECT Mapping.PRICE/Mapping.MQ FROM Mapping WHERE Opportunity.ID = Mapping.ID),
            SCOSTAMENTO = (select ABBATTIMENTO from (select case when Averages.RANGE = '0_50' OR Averages.RANGE = '51_70' then (Mapping.Price - Averages.PRICE_ABBATTUTO) / Averages.PRICE_ABBATTUTO
            ELSE
            ((Mapping.PRICE / Mapping.MQ) - Averages.PRICE_ABBATTUTO_MQ) / Averages.PRICE_ABBATTUTO_MQ
            END
            as ABBATTIMENTO,Mapping.ID
            from Mapping INNER JOIN  Averages ON 
            Mapping.MICROZONE || "_" || Mapping.RANGE  = Averages.MZ_RANGE
            )Mapping_abbattuto WHERE Mapping_abbattuto.ID = Opportunity.ID),
            UPDATED = (SELECT CASE WHEN Opportunity.PRICE > Mapping.PRICE THEN 'RIBASSATO' WHEN Opportunity.PRICE < Mapping.PRICE THEN 'RIALZATO' ELSE FALSE END FROM Mapping where Mapping.ID = Opportunity.ID)
            WHERE ID in (SELECT distinct Opportunity.ID FROM Opportunity join Mapping on Opportunity.ID = Mapping.ID)          
            """)
    connection.commit()
    connection.close()

def insertAveragesIfOccurs(connection,type):
    cursor = connection.cursor()
    if type == 'mysql_old':
        cursor.execute("""
        INSERT INTO Averages
        SELECT DISTINCT concat(Mapping.MICROZONE,"_", Mapping.RANGE),Mapping.MICROZONE, Mapping.RANGE, Ranges.ABBATTIMENTO, NULL,NULL, NULL,NULL,count(ID)
        from Mapping join Ranges on Mapping.RANGE = Ranges.RANGE
        WHERE concat(Mapping.MICROZONE,"_", Mapping.RANGE) NOT IN (SELECT DISTINCT MZ_RANGE FROM Averages)
        GROUP BY Mapping.MICROZONE, Mapping.RANGE, Ranges.ABBATTIMENTO;
        """)
    elif type == 'mysql':
        cursor.execute("""
        INSERT INTO Averages
        SELECT DISTINCT concat(Mapping.MICROZONE,'_', Mapping.RANGE),Mapping.MICROZONE, Mapping.RANGE, Ranges.ABBATTIMENTO, NULL,NULL, NULL,NULL,count(ID)
        from Mapping join Ranges on Mapping.RANGE = Ranges.RANGE
        WHERE concat(Mapping.MICROZONE,'_', Mapping.RANGE) NOT IN (SELECT DISTINCT MZ_RANGE FROM Averages)
        GROUP BY Mapping.MICROZONE, Mapping.RANGE, Ranges.ABBATTIMENTO;
        """)
    elif type == 'sqlite':
        cursor.execute(
            """
        INSERT INTO Averages
        SELECT DISTINCT Mapping.MICROZONE || "_" || Mapping.RANGE ,Mapping.MICROZONE, Mapping.RANGE, Ranges.ABBATTIMENTO, NULL,NULL, NULL,NULL,count(ID)
        from Mapping join Ranges on Mapping.RANGE = Ranges.RANGE
        WHERE Mapping.MICROZONE || "_" || Mapping.RANGE NOT IN (SELECT DISTINCT MZ_RANGE FROM Averages)
        GROUP BY Mapping.MICROZONE, Mapping.RANGE, Ranges.ABBATTIMENTO;
            """
        )
    
    connection.commit()
    connection.close()

def insertMappingToSQL(connection, rows,type):
    cursor = connection.cursor()
    
        # print(
        # """INSERT INTO Mapping \
        #     VALUES \
        #     ( \
        #     '""" + row.Country + """', \
        #     '""" + row.Region + """', \
        #     '""" + row.Province + """', \
        #     '""" + row.City + """',\
        #     '""" + row.Microzone + """', \
        #     '""" + row.id + """', \
        #     '""" + row.url + """', \
        #     '""" + row.Agency + """', \
        #     '""" + row.Address + """', \
        #     '""" + str(row.MQ) + """', \
        #     '""" + row.Range + """', \
        #     '""" + str(row.Floor) + """', \
        #     """ + str(row.Auction) + """, \
        #     """ + str(row.Toilet) + """, \
        #     '""" + row.Date + """', \
        #     '""" + row.Price + """'); \
        #     """
        #     )
        # print(row)
    if type == 'mysql_old':
        for row in rows:
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
            '""" + str(row.Agency) + """', \
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
    elif type == 'mysql':
        sql = "INSERT INTO MAPPING (COUNTRY,REGION,PROVINCE,CITY,MICROZONE,ID,URL,AGENCY,ADDRESS,MQ,RANGE,FLOOR,AUCTION,TOILET,DATA,PRICE) VALUES ( ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        data_tuple = []
        for row in rows:
            #print(row)
            data_tuple.append([ 
            row.Country, 
            row.Region,
            row.Province,
            row.City,
            row.Microzone, 
            row.id,
            row.url,
            str(row.Agency),
            row.Address,
            str(row.MQ),
            row.Range,
            str(row.Floor),
            str(row.Auction),
            str(row.Toilet),
            row.Date,
            row.Price
            ]
            )
        cursor.fast_executemany = True
        cursor.executemany(sql,data_tuple)
    elif type == 'sqlite':        
        sql = "INSERT INTO Mapping (COUNTRY,REGION,PROVINCE,CITY,MICROZONE,ID,URL,AGENCY,ADDRESS,MQ,RANGE,FLOOR,AUCTION,TOILET,DATA,PRICE) VALUES ( ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
        data_tuple = []
        for row in rows:
            data_tuple.append([
            row.Country, 
            row.Region,
            row.Province,
            row.City,
            row.Microzone, 
            row.id,
            row.url,
            str(row.Agency),
            row.Address,
            str(row.MQ),
            row.Range,
            str(row.Floor),
            str(row.Auction),
            str(row.Toilet),
            row.Date,
            row.Price
            ]
            )
        cursor.executemany(sql,data_tuple)
    connection.commit()
    connection.close()

def insertMappingStoryToSQL(connection,type):
    cursor = connection.cursor()
    if type == 'mysql':
        sql = "INSERT INTO Mapping_story SELECT Mapping.* FROM Mapping LEFT JOIN Mapping_story on Mapping.Data = Mapping_story.DATA where Mapping_story.DATA is null"
    elif type == 'sqlite':
        sql = "INSERT INTO Mapping_story SELECT Mapping.* FROM Mapping LEFT JOIN Mapping_story on Mapping.Data = Mapping_story.DATA where Mapping_story.DATA is null"
    #print(sql)
    cursor.execute(sql)
    connection.commit()
    connection.close()

def insertAveragesStoryToSQL(connection,type, output_timestamp):
    cursor = connection.cursor()
    if type == 'mysql':
        sql = "INSERT INTO Averages_story SELECT Averages.*,'"+output_timestamp+"' FROM Averages WHERE '"+output_timestamp+"' NOT IN (SELECT DISTINCT DATA FROM Averages_story)"
    elif type == 'sqlite':
        sql = "INSERT INTO Averages_story SELECT Averages.*,'"+output_timestamp+"' FROM Averages WHERE '"+output_timestamp+"' NOT IN (SELECT DISTINCT DATA FROM Averages_story)"
    LOG_insert("file.log", formatLOG , "AVERAGE query: " + sql, logging.INFO)
    cursor.execute(sql)
    connection.commit()
    connection.close()

def getAverages(connection,type):
    cursor = connection.cursor()
    if type == 'mysql':
        sql = "SELECT MZ,RANGE,AVG_PREZZO,AVG_PREZZO_MQ,CNT_MZ FROM Averages "
    elif type == 'sqlite':
        sql = "SELECT MZ,RANGE,AVG_PREZZO,AVG_PREZZO_MQ,CNT_MZ FROM Averages "
    cursor.execute(sql)
    result = cursor.fetchall()
    rows = list( cursor )
    return rows

def insertNewOpportunities(connection,type):
    cursor = connection.cursor()
    if type == 'mysql_old':
        a = cursor.execute(
        """
        insert into Opportunity
        select
        NULL,
        DATA,
        'Nuova',
        COUNTRY,
        REGION,
        PROVINCE,
        CITY,
        MICROZONE,
        ID,
        URL,
        ADDRESS,
        MQ,
        `RANGE`,
        concat(MICROZONE,"_",`RANGE`),
        FLOOR,
        AUCTION,
        TOILET,
        DATA,
        PRICE,
        (PRICE / MQ),
        ABBATTIMENTO,
        AGENCY,
        FALSE,
        FALSE
        FROM
        (select case when Averages.RANGE = '0_50' OR Averages.RANGE = '51_70' then (Mapping.Price - Averages.PRICE_ABBATTUTO) / Averages.PRICE_ABBATTUTO
        ELSE
        ((Mapping.PRICE / Mapping.MQ) - Averages.PRICE_ABBATTUTO_MQ) / Averages.PRICE_ABBATTUTO_MQ
        END
        as ABBATTIMENTO
        , Mapping.* from Mapping join Averages ON
        concat(Mapping.MICROZONE,"_", Mapping.RANGE) = Averages.MZ_RANGE
        )Mapping_abbattuto
        where Mapping_abbattuto.ID not in (select distinct ID from Opportunity)
        and
        Mapping_abbattuto.ABBATTIMENTO <=10
        """
        )
    elif type == 'mysql':
        a = cursor.execute(
        """
        insert into Opportunity
        select
        NULL,
        DATA,
        'Nuova',
        COUNTRY,
        REGION,
        PROVINCE,
        CITY,
        MICROZONE,
        ID,
        URL,
        ADDRESS,
		mq,
        RANGE,
        concat(MICROZONE,'_',RANGE),
        FLOOR,
        AUCTION,
        TOILET,
        DATA,
        PRICE,
        (try_cast(TRY_CAST(PRICE AS NUMERIC) as int))/ (try_cast(TRY_CAST(MQ AS NUMERIC) as int)),
        ABBATTIMENTO,
        AGENCY,
        0,
        0
        FROM
        (select case when Averages.RANGE = '0_50' OR Averages.RANGE = '51_70' then try_cast(try_cast(Mapping.Price as numeric) as int) - (try_cast(try_cast(Averages.PRICE_ABBATTUTO as numeric) as int) / try_cast(try_cast(Averages.PRICE_ABBATTUTO as numeric) as int))
        ELSE
        ((try_cast(TRY_CAST(Mapping.PRICE AS NUMERIC) as float)/ (try_cast(TRY_CAST(Mapping.MQ AS NUMERIC) as float))) - try_cast(try_cast(Averages.PRICE_ABBATTUTO_MQ as numeric) as float)) / try_cast(try_cast(Averages.PRICE_ABBATTUTO_MQ as numeric) as float)
        END
        as ABBATTIMENTO
        , Mapping.* from Mapping join Averages ON
        concat(Mapping.MICROZONE,'_', Mapping.RANGE) = Averages.MZ_RANGE
        )Mapping_abbattuto
        where Mapping_abbattuto.ID not in (select distinct ID from Opportunity)
        and
        Mapping_abbattuto.ABBATTIMENTO <=10
        """
        )
    elif type == 'sqlite':
        a = cursor.execute(
        """
        insert into Opportunity
        select
        NULL,
        DATA,
        'Nuova',
        COUNTRY,
        REGION,
        PROVINCE,
        CITY,
        MICROZONE,
        ID,
        URL,
        ADDRESS,
        MQ,
        RANGE,
        MICROZONE || "_" || RANGE,
        FLOOR,
        AUCTION,
        TOILET,
        DATA,
        PRICE,
        (PRICE / MQ),
        ABBATTIMENTO,
        AGENCY,
        FALSE,
        FALSE
        FROM
        (select case when Averages.RANGE = '0_50' OR Averages.RANGE = '51_70' then (Mapping.Price - Averages.PRICE_ABBATTUTO) / Averages.PRICE_ABBATTUTO
        ELSE
        ((Mapping.PRICE / Mapping.MQ) - Averages.PRICE_ABBATTUTO_MQ) / Averages.PRICE_ABBATTUTO_MQ
        END
        as ABBATTIMENTO , Mapping.*
        from Mapping INNER JOIN  Averages ON 
        Mapping.MICROZONE || "_" || Mapping.RANGE  = Averages.MZ_RANGE
        )Mapping_abbattuto
        where Mapping_abbattuto.ID not in (select distinct ID from Opportunity)
        and
        Mapping_abbattuto.ABBATTIMENTO <=10
        """
        )
    connection.commit()
    connection.close()

def updateExistingOpportunities(connection,type):
    cursor = connection.cursor()
    if type == 'mysql_old':
        a = cursor.execute(
        """
        UPDATE Opportunity
        JOIN
        (select case when Averages.RANGE = '0_50' OR Averages.RANGE = '51_70' then (Mapping.Price - Averages.PRICE_ABBATTUTO) / Averages.PRICE_ABBATTUTO
        ELSE
        ((Mapping.PRICE / Mapping.MQ) - Averages.PRICE_ABBATTUTO_MQ) / Averages.PRICE_ABBATTUTO_MQ
        END
        as ABBATTIMENTO
        , Mapping.* from Mapping join Averages ON
        concat(Mapping.MICROZONE,"_", Mapping.RANGE) = Averages.MZ_RANGE
        )Mapping_abbattuto on Mapping_abbattuto.ID = Opportunity.ID
        SET 
        Opportunity.DATA = Mapping_abbattuto.DATA,
        Opportunity.PRICE = Mapping_abbattuto.PRICE,
        Opportunity.PRICE_MQ = (Mapping_abbattuto.PRICE / Mapping_abbattuto.MQ),
        Opportunity.SCOSTAMENTO = Mapping_abbattuto.ABBATTIMENTO,
        Opportunity.UPDATED = CASE WHEN Opportunity.SCOSTAMENTO < Mapping_abbattuto.ABBATTIMENTO THEN 'RIBASSATO' WHEN Opportunity.SCOSTAMENTO < Mapping_abbattuto.ABBATTIMENTO THEN 'RIALZATO' ELSE FALSE END
        WHERE Mapping_abbattuto.ID = Opportunity.ID and Mapping_abbattuto.PRICE <> Opportunity.PRICE
        """
        )
    elif type == 'mysql':
        """
        UPDATE Opportunity
        SET 
        Opportunity.DATA = Mapping_abbattuto.DATA,
        Opportunity.PRICE = Mapping_abbattuto.PRICE,
        Opportunity.PRICE_MQ = (Mapping_abbattuto.PRICE / Mapping_abbattuto.MQ),
        Opportunity.SCOSTAMENTO = Mapping_abbattuto.ABBATTIMENTO,
        Opportunity.UPDATED = CASE WHEN Opportunity.SCOSTAMENTO < Mapping_abbattuto.ABBATTIMENTO THEN 'RIBASSATO' WHEN Opportunity.SCOSTAMENTO < Mapping_abbattuto.ABBATTIMENTO THEN 'RIALZATO' ELSE FALSE END
        from Opportunity
        JOIN
        (select case when Averages.RANGE = '0_50' OR Averages.RANGE = '51_70' then (Mapping.Price - Averages.PRICE_ABBATTUTO) / Averages.PRICE_ABBATTUTO
        ELSE
        ((Mapping.PRICE / Mapping.MQ) - Averages.PRICE_ABBATTUTO_MQ) / Averages.PRICE_ABBATTUTO_MQ
        END
        as ABBATTIMENTO
        , Mapping.* from Mapping join Averages ON
        concat(Mapping.MICROZONE,"_", Mapping.RANGE) = Averages.MZ_RANGE
        )Mapping_abbattuto on Mapping_abbattuto.ID = Opportunity.ID
        WHERE Mapping_abbattuto.ID = Opportunity.ID and Mapping_abbattuto.PRICE <> Opportunity.PRICE
        """
    elif type == 'sqlite':
        a = cursor.execute(
        """
        UPDATE Opportunity
        SET 
        DATA = (SELECT DATA FROM Mapping WHERE Opportunity.ID = Mapping.ID),
        PRICE = (SELECT PRICE FROM Mapping WHERE Opportunity.ID = Mapping.ID),
        PRICE_MQ = (SELECT PRICE/MQ FROM Mapping WHERE Opportunity.ID = Mapping.ID),
        SCOSTAMENTO = (select ABBATTIMENTO from (select case when Averages.RANGE = '0_50' OR Averages.RANGE = '51_70' then (Mapping.Price - Averages.PRICE_ABBATTUTO) / Averages.PRICE_ABBATTUTO
        ELSE
        ((Mapping.PRICE / Mapping.MQ) - Averages.PRICE_ABBATTUTO_MQ) / Averages.PRICE_ABBATTUTO_MQ
        END
        as ABBATTIMENTO,Mapping.ID
        from Mapping INNER JOIN  Averages ON 
        Mapping.MICROZONE || "_" || Mapping.RANGE  = Averages.MZ_RANGE
        )Mapping_abbattuto WHERE Mapping_abbattuto.ID = Opportunity.ID),
        UPDATED = (SELECT CASE WHEN Opportunity.PRICE > Mapping.PRICE THEN 'RIBASSATO' WHEN Opportunity.PRICE < Mapping.PRICE THEN 'RIALZATO' ELSE FALSE END FROM Mapping where Mapping.ID = Opportunity.ID)
        """
        )
    connection.commit()
    connection.close()


def markLostOpportuntiies(connection,type):
    cursor = connection.cursor()
    if type == 'mysql':
        a = cursor.execute(
            """
            UPDATE Opportunity SET UPDATED = 'LOST OPP!' WHERE ID NOT IN (SELECT DISTINCT ID FROM MAPPING)
            """
        )
    elif type == 'sqlite':
        a = cursor.execute(
            """
            UPDATE Opportunity SET UPDATED = 'LOST OPP!' WHERE ID NOT IN (SELECT DISTINCT ID FROM MAPPING)
            """
        )
    connection.commit()
    connection.close()

##########################################################################


def launch():
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
    data = pd.read_csv('torino.csv', names=colnames, sep="|",engine='python')

    urls = data.url.tolist()
    i = 0
    futures = []

    start_time = time()
    #output_timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    output_timestamp = datetime.datetime.now().strftime("%Y%m%d")
    LOG_insert("file.log", formatLOG , "STARTED ON: " + str(output_timestamp), logging.INFO)
    # path = ROOT_DIR+"\\IMMOBILIARE\\"
    path = os.path.join(ROOT_DIR,"IMMOBILIARE")

    print(path)

    if os.path.exists(path):
        shutil.rmtree(path)
        
    if not os.path.exists(path):
        os.mkdir(path)

    with ThreadPoolExecutor(max_workers=2) as executor:
        for index, row in data.iterrows():
            links = []
            i = i + 1
            # path = ROOT_DIR +"\\IMMOBILIARE\\" + str(row['zone_code']) + \
                # "" + str(row['microzone_code']) + "\\"
            path = os.path.join(ROOT_DIR,"IMMOBILIARE",str(row['zone_code']) + "" + str(row['microzone_code']))
            print(path)
            url = row['url']
            links = get_links_immobiliare(url)
            LOG_insert("file.log", formatLOG , str(i) + " lista da link " + url, logging.INFO)
            #print(str(i) + " lista da link " + url)
            LOG_insert("file.log", formatLOG , str(i) + "totale links: " + str(len(links)), logging.INFO)
            #print("totale links: " + str(len(links)))
            j = 0
            for s in links:
                j = j + 1
                futures.append(
                    executor.submit(get_data_immobiliare, s, path, str(j), row)
                )
    
    # for index, row in data.iterrows():
    #     links = []
    #     i = i + 1
    #     # path = ROOT_DIR +"\\IMMOBILIARE\\" + str(row['zone_code']) + \
    #         # "" + str(row['microzone_code']) + "\\"
    #     path = os.path.join(ROOT_DIR,"IMMOBILIARE",str(row['zone_code']) + "" + str(row['microzone_code']))
    #     print(path)
    #     url = row['url']
    #     links = get_links_immobiliare(url)
    #     print(str(i) + " lista da link " + url)
    #     print("totale links: " + str(len(links)))
    #     j = 0
    #     for s in links:
    #         j = j + 1
    #         get_data_immobiliare(s, path, str(j), row)


    end_time = time()
    elapsed_time = end_time - start_time
    LOG_insert("file.log", formatLOG , str(i) + f"Elapsed run time SCRAPING: {elapsed_time} seconds", logging.INFO)
    print(f"Elapsed run time SCRAPING: {elapsed_time} seconds")

    print("truncate")
    start_time = time()


    truncateSQLTable(connectToSQL('mssql'), 'Mapping','mysql')
    truncateSQLTable(connectToSQL('mssql'), 'Averages','mysql')

    end_time = time()
    elapsed_time = end_time - start_time
    print(f"Elapsed run time TRUNCATE: {elapsed_time} seconds")


    path_to_walk = os.path.join(ROOT_DIR,"IMMOBILIARE")
    rows = []
    for subdir, dirs, files in os.walk(path_to_walk):
        for filename in files:
            filepath = subdir + os.sep + filename
            if filepath.endswith(".json"):
                print(e)
                LOG_insert("file.log", formatLOG , "CLEANING "+ filepath, logging.INFO)
                f = open(filepath)
                d = json.load(f)
                elem = cleanDataImmobiliare(d)
                if elem != None:
                    rows.append(elem)


    print("insertion")
    start_time = time()

    insertMappingToSQL(connectToSQL('mssql'), rows, 'mysql')
    end_time = time()
    elapsed_time = end_time - start_time
    print(f"Elapsed run time INSERTION: {elapsed_time} seconds")

    deleted_rows = cleanAuctions(connectToSQL('mssql'))
    print("Sono state cancellati "+str(deleted_rows)+" records da aste o nuove costruzioni")
    deleted_rows = cleanZeroPrice(connectToSQL('mssql'))
    print("Sono state cancellati "+str(deleted_rows)+" records con price zero")
    deleted_rows = cleanZeroMQ(connectToSQL('mssql'))
    print("Sono state cancellati "+str(deleted_rows)+" records con mq zero")


    updateRangesMapping(connectToSQL('mssql'),"mysql")
    insertAveragesIfOccurs(connectToSQL('mssql'),"mysql")
    updateAverages(connectToSQL('mssql'),"mysql")

    cleanOutOfRange(connectToSQL('mssql'),'mysql')
    updateAverages(connectToSQL('mssql'),"mysql")
    cleanOutOfRange(connectToSQL('mssql'),'mysql')
    updateAverages(connectToSQL('mssql'),"mysql")

    insertMappingStoryToSQL(connectToSQL('mssql'),"mysql")
    insertAveragesStoryToSQL(connectToSQL('mssql'),"mysql",datetime.datetime.now().strftime("%Y%m%d"))

    insertNewOpportunities(connectToSQL('mssql'),"mysql")

    updateExistingOpportunities(connectToSQL('mssql'),"mysql")
    markLostOpportuntiies(connectToSQL('mssql'),"mysql")

    # ########################################################################################################################

    # conn = connectToSQLite()



    # if conn != None:
    #     c = conn.cursor()
    #     c.execute(
    #         """
    #         CREATE TABLE IF NOT EXISTS `Mapping` (
    #         `COUNTRY` varchar(50) NOT NULL,
    #         `REGION` varchar(50) NOT NULL,
    #         `PROVINCE` varchar(50) NOT NULL,
    #         `CITY` varchar(50) NOT NULL,
    #         `MICROZONE` varchar(10) NOT NULL,
    #         `ID` varchar(50) NOT NULL,
    #         `URL` varchar(200) NOT NULL,
    #         `AGENCY` varchar(100) DEFAULT NULL,
    #         `ADDRESS` varchar(100) NOT NULL,
    #         `MQ` varchar(10) DEFAULT NULL,
    #         `RANGE` varchar(50) NOT NULL,
    #         `FLOOR` varchar(100) NOT NULL,
    #         `AUCTION` int(1) NOT NULL,
    #         `TOILET` int(1) NOT NULL,
    #         `DATA` varchar(10) DEFAULT NULL,
    #         `PRICE` varchar(20)
    #         );
    #         """
    #     )
    #     c.execute(
    #         """
    #         CREATE TABLE IF NOT EXISTS `Averages` (
    #         `MZ_RANGE` varchar(20) NOT NULL,
    #         `MZ` varchar(10) NOT NULL,
    #         `RANGE` varchar(10) NOT NULL,
    #         `ABBATTIMENTO` float NOT NULL,
    #         `AVG_PREZZO` int(11) DEFAULT NULL,
    #         `AVG_PREZZO_MQ` int(11) DEFAULT NULL,
    #         `PRICE_ABBATTUTO` int(11) DEFAULT NULL,
    #         `PRICE_ABBATTUTO_MQ` int(11) DEFAULT NULL,
    #         `CNT_MZ` int(11) DEFAULT NULL
    #         );
    #         """
    #     )
    #     c.execute(
    #         """
    #         CREATE TABLE IF NOT EXISTS `Opportunity` (
    #         `OWNER` varchar(50) DEFAULT NULL,
    #         `DATA_OPP` varchar(10) DEFAULT NULL,
    #         `STATO_OPP` varchar(50) DEFAULT NULL,
    #         `COUNTRY` varchar(50) NOT NULL,
    #         `REGION` varchar(50) NOT NULL,
    #         `PROVINCE` varchar(50) NOT NULL,
    #         `CITY` varchar(50) NOT NULL,
    #         `MICROZONE` varchar(10) NOT NULL,
    #         `ID` varchar(50) NOT NULL,
    #         `URL` varchar(200) NOT NULL,
    #         `ADDRESS` varchar(100) NOT NULL,
    #         `MQ` varchar(10) DEFAULT NULL,
    #         `RANGE` varchar(50) NOT NULL,
    #         `MZ_RANGE` varchar(20) NOT NULL,
    #         `PIANO` varchar(100) NOT NULL,
    #         `AUCTION` int(1) NOT NULL,
    #         `TOILET` int(11) NOT NULL,
    #         `DATA` varchar(10) DEFAULT NULL,
    #         `PRICE` varchar(20),
    #         `PRICE_MQ` varchar(20) NULL,
    #         `SCOSTAMENTO` float DEFAULT NULL,
    #         `AGENZIA` varchar(50) DEFAULT NULL,
    #         `UPDATED` varchar(50) NULL,
    #         `IS_DELETED` tinyint(1) NOT NULL
    #         );
    #         """
    #     )
    #     c.execute(
    #     """
    #     CREATE TABLE IF NOT EXISTS `Ranges` (
    #         `MIN` int(11) NOT NULL,
    #         `MAX` int(11) NOT NULL,
    #         `RANGE` varchar(10) NOT NULL,
    #         `ABBATTIMENTO` float NOT NULL
    #         );
    #     """
    #     )
    #     c.execute(
    #     """
    #     INSERT INTO `ranges` (`MIN`, `MAX`, `RANGE`, `ABBATTIMENTO`) VALUES
    #     (0, 50, '0_50', 0.8),
    #     (51, 70, '51_70', 0.8),
    #     (71, 90, '71_90', 0.7),
    #     (91, 110, '91_110', 0.7),
    #     (111, 500, '111_500', 0.7);
    #     """
    #     )
    #     c.execute(
    #     """
    #     CREATE TABLE IF NOT EXISTS `Mapping_story` (
    #     `COUNTRY` varchar(50) NOT NULL,
    #     `REGION` varchar(50) NOT NULL,
    #     `PROVINCE` varchar(50) NOT NULL,
    #     `CITY` varchar(50) NOT NULL,
    #     `MICROZONE` varchar(10) NOT NULL,
    #     `ID` varchar(50) NOT NULL,
    #     `URL` varchar(200) NOT NULL,
    #     `AGENCY` varchar(100) DEFAULT NULL,
    #     `ADDRESS` varchar(100) NOT NULL,
    #     `MQ` varchar(10) DEFAULT NULL,
    #     `RANGE` varchar(50) NOT NULL,
    #     `FLOOR` varchar(100) DEFAULT NULL,
    #     `AUCTION` int(1) NOT NULL,
    #     `TOILET` int(11) NOT NULL,
    #     `DATA` varchar(10) DEFAULT NULL,
    #     `PRICE` varchar(20)
    #     );
    #     """
    #     )
    #     c.execute(
    #         """
    #         CREATE TABLE IF NOT EXISTS `Averages_story` (
    #         `MZ_RANGE` varchar(20) NOT NULL,
    #         `MZ` varchar(10) NOT NULL,
    #         `RANGE` varchar(10) NOT NULL,
    #         `ABBATTIMENTO` float NOT NULL,
    #         `AVG_PREZZO` int(11) DEFAULT NULL,
    #         `AVG_PREZZO_MQ` int(11) DEFAULT NULL,
    #         `PRICE_ABBATTUTO` int(11) DEFAULT NULL,
    #         `PRICE_ABBATTUTO_MQ` int(11) DEFAULT NULL,
    #         `CNT_MZ` int(11) DEFAULT NULL,
    #         `DATA` varchar(10) DEFAULT NULL
    #         );
    #         """
    #     )
    # conn.commit()

    # truncateSQLTable(connectToSQLite(), 'Mapping','sqlite')
    # truncateSQLTable(connectToSQLite(), 'Averages','sqlite')

    # insertMappingToSQL(connectToSQLite(), rows, 'sqlite')
    # cleanAuctions(connectToSQLite())
    # cleanZeroPrice(connectToSQLite())
    # updateRangesMapping(connectToSQLite(),"sqlite")
    # insertAveragesIfOccurs(connectToSQLite(),"sqlite")
    # updateAverages(connectToSQLite(),"sqlite")

    # cleanOutOfRange(connectToSQLite(),'sqlite')
    # updateAverages(connectToSQLite(),"sqlite")
    # cleanOutOfRange(connectToSQLite(),'sqlite')
    # updateAverages(connectToSQLite(),"sqlite")

    # insertMappingStoryToSQL(connectToSQLite(),"sqlite")
    # insertAveragesStoryToSQL(connectToSQLite(),"sqlite",datetime.datetime.now().strftime("%Y%m%d"))

    # insertNewOpportunities(connectToSQLite(),"sqlite")


    # updateExistingOpportunities(connectToSQLite(),"sqlite")
    # markLostOpportuntiies(connectToSQLite(),"sqlite")
    # conn.close()


def onlyinsert():
    ROOT_DIR = os.path.dirname(os.path.abspath(__file__)) 
    DIRECTORY = os.environ.get("DIRECTORY")

    print("truncate")
    start_time = time()


    truncateSQLTable(connectToSQL('mssql'), 'Mapping','mysql')
    truncateSQLTable(connectToSQL('mssql'), 'Averages','mysql')

    end_time = time()
    elapsed_time = end_time - start_time
    print(f"Elapsed run time TRUNCATE: {elapsed_time} seconds")


    path_to_walk = os.path.join(ROOT_DIR,"IMMOBILIARE")
    rows = []
    for subdir, dirs, files in os.walk(path_to_walk):
        for filename in files:
            filepath = subdir + os.sep + filename
            if filepath.endswith(".json"):
                print(e)
                LOG_insert("file.log", formatLOG , "CLEANING "+ filepath, logging.INFO)
                f = open(filepath)
                d = json.load(f)
                elem = cleanDataImmobiliare(d)
                if elem != None:
                    rows.append(elem)


    print("insertion")
    start_time = time()

    insertMappingToSQL(connectToSQL('mssql'), rows, 'mysql')
    end_time = time()
    elapsed_time = end_time - start_time
    print(f"Elapsed run time INSERTION: {elapsed_time} seconds")

    deleted_rows = cleanAuctions(connectToSQL('mssql'))
    print("Sono state cancellati "+str(deleted_rows)+" records da aste o nuove costruzioni")
    deleted_rows = cleanZeroPrice(connectToSQL('mssql'))
    print("Sono state cancellati "+str(deleted_rows)+" records con price zero")
    deleted_rows = cleanZeroMQ(connectToSQL('mssql'))
    print("Sono state cancellati "+str(deleted_rows)+" records con mq zero")


    updateRangesMapping(connectToSQL('mssql'),"mysql")
    insertAveragesIfOccurs(connectToSQL('mssql'),"mysql")
    updateAverages(connectToSQL('mssql'),"mysql")

    cleanOutOfRange(connectToSQL('mssql'),'mysql')
    updateAverages(connectToSQL('mssql'),"mysql")
    cleanOutOfRange(connectToSQL('mssql'),'mysql')
    updateAverages(connectToSQL('mssql'),"mysql")

    insertMappingStoryToSQL(connectToSQL('mssql'),"mysql")
    insertAveragesStoryToSQL(connectToSQL('mssql'),"mysql",datetime.datetime.now().strftime("%Y%m%d"))

    insertNewOpportunities(connectToSQL('mssql'),"mysql")

    updateExistingOpportunities(connectToSQL('mssql'),"mysql")
    markLostOpportuntiies(connectToSQL('mssql'),"mysql")
