import requests
import pandas as pd

def fetchBasicData(URL):
    #Sends get request and saves response as response object
    r = requests.get(url = f"{URL}")

    #Checks if there is no invalid bad request, then converts to JSON
    if(r.status_code != requests.codes.ok):
        print(r.raise_for_status())
    data = r.json()
    return data