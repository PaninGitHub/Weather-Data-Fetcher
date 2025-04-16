import requests
import numpy
import pandas as pd
import aiohttp 

def fetchBasicData(URL):
    #Sends get request and saves response as response object
    r = requests.get(url = f"{URL}")
    #Checks if there is no invalid bad request, then converts to JSON
    if(r.status_code != requests.codes.ok):
        print(r.raise_for_status())
        return None
    data = r.json()
    return data


#We assume that there exists a session
async def fetchBasicDataAsync(URL, session: aiohttp.ClientSession):
    #Sends async get request
    async with session.get(f"{URL}") as res:
        #Checks if there is no invalid bad request, then converts to JSON
        if res.status != 200:
            print("Error with async called in fetchBasicDataAsync: Got status", res.status)
        r = await res.json()
        return r
    
