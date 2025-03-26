import requests
import pandas as pd
from datetime import datetime, timedelta
from .helper import fetchBasicData

def getStations(URL):
    #Fetch Data
    data = fetchBasicData(URL)
    stations = []
    #Iterate
    for key, value in data.items():
        print(type(value))
        for item, item_value in value.items():
            if item == "features":
                for feature in item_value:
                    stations.append(feature["id"])
    #Convert to set to remove duplicates
    stations = set(stations)
    return stations

def extractData(_data):
    temp = []
    for observation in _data['observations']:
        # Extract and append the necessary properties
        observation_data = {
            "stationID": observation["stationID"],
            "obsTimeUtc": observation["obsTimeUtc"],
            "lat": observation["lat"],
            "lon": observation["lon"],
            "solarRadiationHigh": observation["solarRadiationHigh"],
            "uvHigh": observation["uvHigh"],
            "tempHigh": observation["imperial"]["tempHigh"],
            "tempLow": observation["imperial"]["tempLow"],
            "tempAvg": observation["imperial"]["tempAvg"],
            "windspeedHigh": observation["imperial"]["windspeedHigh"],
            "windspeedLow": observation["imperial"]["windspeedLow"],
            "windspeedAvg": observation["imperial"]["windspeedAvg"],
            "windgustHigh": observation["imperial"]["windgustHigh"],
            "windgustLow": observation["imperial"]["windgustLow"],
            "windgustAvg": observation["imperial"]["windgustAvg"],
            "dewptHigh": observation["imperial"]["dewptHigh"],
            "dewptLow": observation["imperial"]["dewptLow"],
            "dewptAvg": observation["imperial"]["dewptAvg"],
            "windchillHigh": observation["imperial"]["windchillHigh"],
            "windchillLow": observation["imperial"]["windchillLow"],
            "windchillAvg": observation["imperial"]["windchillAvg"],
            "heatindexHigh": observation["imperial"]["heatindexHigh"],
            "heatindexLow": observation["imperial"]["heatindexLow"],
            "heatindexAvg": observation["imperial"]["heatindexAvg"],
            "pressureMax": observation["imperial"]["pressureMax"],
            "pressureMin": observation["imperial"]["pressureMin"],
            "pressureTrend": observation["imperial"]["pressureTrend"],
            "precipRate": observation["imperial"]["precipRate"],
            "precipTotal": observation["imperial"]["precipTotal"]
        }
        temp.append(observation_data)
    return temp

def getDay(date, station):
    URL = f"https://api.weather.com/v2/pws/history/all?stationId={station}&format=json&units=e&date={date}&numericPrecision=decimal&apiKey=e1f10a1e78da46f5b10a1e78da96f525"
    data = fetchBasicData(URL)
    if(data == None):
        return pd.DataFrame()   
    return pd.DataFrame(extractData(data))

def getDays(start_date, end_date, station):
    #Repeatedly runs until it hits end
    ds = pd.DataFrame()
    cur_start = datetime.strptime(start_date, "%Y%m%d")
    end_date = datetime.strptime(end_date, "%Y%m%d")
    cur_end = end_date
    
    while cur_start < end_date:
        #Calculates change
        remaining_days = (end_date - cur_start).days
        cur_end = cur_start + timedelta(days=min(30, remaining_days))

        #Call API
        URL = f"https://api.weather.com/v2/pws/history/daily?stationId={station}&format=json&units=e&startDate={cur_start.strftime("%Y%m%d")}&endDate={cur_end.strftime("%Y%m%d")}&numericPrecision=decimal&apiKey=e1f10a1e78da46f5b10a1e78da96f525"
        data = fetchBasicData(URL)
        if(data == None):
            return pd.DataFrame()        
        temp = pd.DataFrame(extractData(data))
        ds = pd.concat([ds, temp], ignore_index=True)
        cur_start = cur_end
    
    return pd.DataFrame(ds)

def getDayofAllStations(date, stations):
    ds = pd.DataFrame()
    for st in stations:
        ds = pd.concat([ds, getDay(date, st)], ignore_index=True)
    return ds

def getDaysofAllStations(start_date, end_date, stations):
    ds = pd.DataFrame()
    for st in stations:
        print("Fetching data from station:", st)
        ds = pd.concat([ds, getDays(start_date, end_date, st)], ignore_index=True)
    return ds