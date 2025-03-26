import requests
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from .helper import fetchBasicData

#Predefines each variable for efficent memory allocation. U50 means a string of max 50, U100 means a string of max 100.
dtype = [
    ("stationID", "U50"), ("obsTimeUtc", "U100"),
    ("lat", "f8"), ("lon", "f8"),
    ("solarRadiationHigh", "f8"), ("uvHigh", "f8"),
    ("tempHigh", "f8"), ("tempLow", "f8"), ("tempAvg", "f8"),
    ("windspeedHigh", "f8"), ("windspeedLow", "f8"), ("windspeedAvg", "f8"),
    ("windgustHigh", "f8"), ("windgustLow", "f8"), ("windgustAvg", "f8"),
    ("dewptHigh", "f8"), ("dewptLow", "f8"), ("dewptAvg", "f8"),
    ("windchillHigh", "f8"), ("windchillLow", "f8"), ("windchillAvg", "f8"),
    ("heatindexHigh", "f8"), ("heatindexLow", "f8"), ("heatindexAvg", "f8"),
    ("pressureMax", "f8"), ("pressureMin", "f8"), ("pressureTrend", "f8"),
    ("precipRate", "f8"), ("precipTotal", "f8")
]

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
    observations = _data['observations']



    #Appends data into a Numpy array efficently using the data types provided above.
    data_array = np.array([
        (
            obs["stationID"], obs["obsTimeUtc"],
            obs["lat"], obs["lon"],
            obs["solarRadiationHigh"], obs["uvHigh"],
            obs["imperial"]["tempHigh"], obs["imperial"]["tempLow"], obs["imperial"]["tempAvg"],
            obs["imperial"]["windspeedHigh"], obs["imperial"]["windspeedLow"], obs["imperial"]["windspeedAvg"],
            obs["imperial"]["windgustHigh"], obs["imperial"]["windgustLow"], obs["imperial"]["windgustAvg"],
            obs["imperial"]["dewptHigh"], obs["imperial"]["dewptLow"], obs["imperial"]["dewptAvg"],
            obs["imperial"]["windchillHigh"], obs["imperial"]["windchillLow"], obs["imperial"]["windchillAvg"],
            obs["imperial"]["heatindexHigh"], obs["imperial"]["heatindexLow"], obs["imperial"]["heatindexAvg"],
            obs["imperial"]["pressureMax"], obs["imperial"]["pressureMin"], obs["imperial"]["pressureTrend"],
            obs["imperial"]["precipRate"], obs["imperial"]["precipTotal"]
        ) for obs in observations
    ], dtype=dtype)
    return data_array

def getDay(date, station, convertToDF):
    URL = f"https://api.weather.com/v2/pws/history/all?stationId={station}&format=json&units=e&date={date}&numericPrecision=decimal&apiKey=e1f10a1e78da46f5b10a1e78da96f525"
    data = fetchBasicData(URL)
    if(convertToDF):
        return pd.DataFrame(extractData(data))
    return extractData(data)

def getDays(start_date, end_date, station, convertToDF):
    temp_data = []  # Use a list instead of concatenating DataFrames

    #Repeatedly runs until it hits end
    cur_start = datetime.strptime(start_date, "%Y%m%d")
    end_date = datetime.strptime(end_date, "%Y%m%d")
    cur_end = end_date
    
    while cur_start < end_date:
        #Calculates change
        remaining_days = (end_date - cur_start).days
        cur_end = cur_start + timedelta(days=min(30, remaining_days))

        #Call API
        URL = f"https://api.weather.com/v2/pws/history/daily?stationId={station}&format=json&units=e&startDate={cur_start.strftime("%Y%m%d")}&endDate={cur_end.strftime("%Y%m%d")}&numericPrecision=decimal&apiKey=e1f10a1e78da46f5b10a1e78da96f525"
        temp_data.append(extractData(fetchBasicData(URL)))
        cur_start = cur_end

    ds = np.array(temp_data, dtype="object").flatten() #Flattens data since temp_data is of type [[Dictionary]]. Also must specify it's an object otherwise the error "setting array element w/ sequence" is thrown.
    if(convertToDF):
        return pd.DataFrame(ds)
    return ds

def getDayofAllStations(date, stations):
    ds = [np.empty((0,), dtype=object)]
    for st in stations:
        print(st)
        ds = np.append(ds, getDay(date, st, False))
    return pd.DataFrame(ds.flatten())

def getDaysofAllStations(start_date, end_date, stations):
    ds = [np.empty((0,), dtype=object)]
    for st in stations:
        print("Now on station:", st)
        ds = np.append(ds, getDays(start_date, end_date, st, False))
    
    #Handles the case where data is formatted differently if the range is more than 30 days (The API's limit)
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    remaining_days = (end - start).days
    if(remaining_days > 30):
        return pd.DataFrame(np.concatenate(ds))
    return pd.DataFrame(ds)