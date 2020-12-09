import requests
from bs4 import BeautifulSoup
import pandas as pd
import csv
import json
from concurrent.futures import ThreadPoolExecutor, wait
import datetime
from time import sleep, time
import os
from dotenv import load_dotenv, find_dotenv



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
            except:
                continue
    return links

def get_data_immobiliare(web_addr,path,name):
    if not os.path.exists(path):
        os.mkdir(path)
    
    r = connect(web_addr)
    payload = r.find(id = 'js-hydration')
    json_object = json.loads(payload.contents[0])
    with open(path+"\\"+name+'.json', 'w') as outfile:
        json.dump(json_object, outfile)   

load_dotenv()
DIRECTORY = os.environ.get("DIRECTORY")

colnames = ['source','country_code','region_code','province_code', 'city_code', 'zone_code', 'zone_desc', 'microzone_code','url','key']
data = pd.read_csv('torino.csv', names=colnames, sep=",")

urls = data.url.tolist()

i = 0
futures = []

start_time = time()
output_timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

path = DIRECTORY

if not os.path.exists(path):
    os.mkdir(path)

with ThreadPoolExecutor() as executor:
    for url in urls:
        sleep(2)
        links=[]
        i = i + 1
        path = DIRECTORY +str(i)
        links = get_links_immobiliare(url)
        print(str(i)+ " lista da link "+ url)
        j = 0
        for s in links:
            j = j + 1
            futures.append(
                executor.submit(get_data_immobiliare,s,path,str(j))
            )

wait(futures)


end_time = time()
elapsed_time = end_time - start_time
print(f"Elapsed run time: {elapsed_time} seconds")
