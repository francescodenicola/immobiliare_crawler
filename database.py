from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker, Query
import urllib
import pyodbc
from sqlalchemy.sql import select
import sqlalchemy as db
import os

DB_SERVER_NAME = os.environ.get("DB_SERVER")
DB_SERVER_PORT = os.environ.get("DB_PORT")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USERNAME")
DB_PWD = os.environ.get("DB_PASSWORD")

# MARS_Connection=yes -> Multiple Active Results Sets (MARS) Enabled
# We inserted it to have the possibility to do multiple sql queries inside
# the loop in (toggle_visibility_for_customer) in suppliers.py
params = urllib.parse.quote_plus(
    r'Driver={ODBC Driver 17 for SQL Server};Server=tcp:' +
    DB_SERVER_NAME+','+DB_SERVER_PORT+';Database='+DB_NAME+';Uid='+DB_USER+';Pwd='+DB_PWD+';Encrypt=yes;TrustServerCertificate=no;MARS_Connection=yes;Connection Timeout=30;')

#Connection string
conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
engine = create_engine(conn_str, echo=False, fast_executemany=True)

Session = sessionmaker(bind=engine)

Base = declarative_base()


#Generate a new session
def get_Session():

    try:
        params = urllib.parse.quote_plus(
            r'Driver={ODBC Driver 17 for SQL Server};Server=tcp:' +
            DB_SERVER_NAME+','+DB_SERVER_PORT+';Database='+DB_NAME+';Uid='+DB_USER+';Pwd='+DB_PWD+';Encrypt=yes;TrustServerCertificate=no;MARS_Connection=yes;Connection Timeout=30;')
            
        #Connection string
        conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
        engine = create_engine(conn_str, echo=False, fast_executemany=True)      
        
        sessionmaker_ = sessionmaker(bind=engine)
        Session = sessionmaker_()
        
        # Set default schema for configured DB user
        # print ("[INFO] : Set default schema to ["+schema+"] for user '"+DB_USER+"'")
        # Session.execute("""ALTER USER """+DB_USER +""" WITH DEFAULT_SCHEMA = """+schema +""" """)

    except Exception as e :
        print ("[ERROR] : " + str(e))

    return Session
