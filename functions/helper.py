import requests
import numpy
import pandas as pd

def fetchBasicData(URL):
    #Sends get request and saves response as response object
    r = requests.get(url = f"{URL}")
    #Checks if there is no invalid bad request, then converts to JSON
    if(r.status_code != requests.codes.ok):
        print(r.raise_for_status())
        return None
    data = r.json()
    return data

async def fetchBasicDataAsync(URL):
    #Sends async get request
    async with requests.get(url = f"{URL}") as r:
        #Checks if there is no invalid bad request
        if r.status_code != requests.codes.ok:
            r.raise_for_status()
            return await None
        #If it's good, return 
        return await r.json()
    
