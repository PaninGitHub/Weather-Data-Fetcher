import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from .helper import fetchBasicData, fetchBasicDataAsync
from dotenv import load_dotenv
import os 
import asyncio
import aiohttp
import time
"""
Fetches data from the API and gathers the station ids from each item in the array of features containing the points on the map 
"""
def getStations(URL):
    #Fetch Data
    data = fetchBasicData(URL)
    stations = []
    #Iterate
    for key, value in data.items():
        for item, item_value in value.items():
            if item == "features":
                for feature in item_value:
                    stations.append(feature["id"])
    #Convert to set to remove duplicates
    stations = set(stations)
    return stations

