import requests
import pandas as pd
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
    return pd.DataFrame(extractData(data))

def getDays(start_date, end_date, station):
    URL = f"https://api.weather.com/v2/pws/history/daily?stationId={station}&format=json&units=e&startDate={start_date}&endDate={end_date}&numericPrecision=decimal&apiKey=e1f10a1e78da46f5b10a1e78da96f525"
    data = fetchBasicData(URL)
    return pd.DataFrame(extractData(data))

def getDayofAllStations(date, stations):
    ds = pd.DataFrame()
    for st in stations:
        ds = pd.concat([ds, getDay(date, st)], ignore_index=True)
    return ds