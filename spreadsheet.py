from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from utils.sqlite import create
from dotenv import load_dotenv, find_dotenv

load_dotenv()
ROOT_DIR = os.path.dirname(os.path.abspath(__file__)) 

GOOGLE_CREDENTIALS = os.environ.get("GOOGLE_CREDENTIALS")
# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = '1U5hSTpRVXh-LHPW-b2rVAkzq66kcodCrTywG12IMVMM'


def getAverages(connection,type):
    cursor = connection.cursor()
    if type == 'mysql':
        sql = "SELECT MZ,RANGE,AVG_PREZZO,AVG_PREZZO_MQ,CNT_MZ FROM Averages "
    elif type == 'sqlite':
        sql = "SELECT MZ,RANGE,AVG_PREZZO,AVG_PREZZO_MQ,CNT_MZ FROM Averages "
    cursor.execute(sql)
    results = cursor.fetchall()
    python_list = []
    for row in results:
        python_list.append(row)
    return python_list


def Averages():
    creds = None
    SHEET_NAME = 'Average'
    SAMPLE_RANGE_NAME_CHECK = SHEET_NAME+'!A3:I'
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                GOOGLE_CREDENTIALS+'\\credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                range=SAMPLE_RANGE_NAME_CHECK).execute()
    values = result.get('values', [])

    i=1
    for row in values:
        # Print columns A and E, which correspond to indices 0 and 4.
        #print('%s, %s' % (row[0], row[4]))
        #print(row)
        i+=1

    try:
        last_row = values[-1]
        print(last_row)
        SAMPLE_RANGE_NAME_CLEAR = SHEET_NAME+'!C3:I'+str(i+1)
        print(SAMPLE_RANGE_NAME_CLEAR)
    
        resultClear = service.spreadsheets( ).values( ).clear(spreadsheetId=SAMPLE_SPREADSHEET_ID, range=SAMPLE_RANGE_NAME_CLEAR).execute()
    except:
        pass
    conn = create.create_connection(ROOT_DIR+"\\utils\\sqlite\\immobiliare_crawler.db")
    rows = getAverages(conn,'sqlite')
    x = 3 #starting row
    for row in rows:
        print(row)
        inp = []
        inp.append(row)
        request = sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID,
        range=SHEET_NAME+'!C'+str(x)+':G'+str(x),valueInputOption="USER_ENTERED",body={"values":inp}).execute()
        x+=1
        
def Opportunity:


def Mapping:
    creds = None
    SHEET_NAME = 'Map'
    SAMPLE_RANGE_NAME_CHECK = SHEET_NAME+'!A3:I'
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                GOOGLE_CREDENTIALS+'\\credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                range=SAMPLE_RANGE_NAME_CHECK).execute()
    values = result.get('values', [])

    i=1
    for row in values:
        # Print columns A and E, which correspond to indices 0 and 4.
        #print('%s, %s' % (row[0], row[4]))
        #print(row)
        i+=1

    try:
        last_row = values[-1]
        print(last_row)
        SAMPLE_RANGE_NAME_CLEAR = SHEET_NAME+'!C
        3:I'+str(i+1)
        print(SAMPLE_RANGE_NAME_CLEAR)
    
        resultClear = service.spreadsheets( ).values( ).clear(spreadsheetId=SAMPLE_SPREADSHEET_ID, range=SAMPLE_RANGE_NAME_CLEAR).execute()
    except:
        pass
    conn = create.create_connection(ROOT_DIR+"\\utils\\sqlite\\immobiliare_crawler.db")
    rows = getAverages(conn,'sqlite')
    x = 3 #starting row
    for row in rows:
        print(row)
        inp = []
        inp.append(row)
        request = sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID,
        range=SHEET_NAME+'!C'+str(x)+':G'+str(x),valueInputOption="USER_ENTERED",body={"values":inp}).execute()
        x+=1



if __name__ == '__main__':
    Averages()