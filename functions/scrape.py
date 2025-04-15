import requests
import pandas as pd
from datetime import datetime, timedelta
from .helper import fetchBasicData
from dotenv import load_dotenv
import os 


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

"""
Takes in JSON object data of a weather station fetched from the API and converts
each observation/feature into an object.

Returns a list of newly formatted observations.
"""
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

"""
Gets infomation on a station on a certain day. 

Returns a pandas Dataframe of the data containing hour increments of data throughout the specified day
"""
def getDay(date, station):
    load_dotenv()
    URL = f"https://api.weather.com/v2/pws/history/all?stationId={station}&format=json&units=e&date={date}&numericPrecision=decimal&apiKey={os.getenv('WEATHER_API_KEY')}"
    data = fetchBasicData(URL)
    if(data == None):
        return pd.DataFrame()   
    return pd.DataFrame(extractData(data))

"""
Gets infomation on a station on a section of time.

The function calls the API in month-long increments, and concateantes each entry into
a DataFrame until all days are covered. Returns the resulting DataFrame

The start_date and end_date parameters must be in YYYYMMDD

Example:
Feburary 19th 2011 -> 20110219
1/1/2001 -> 20010101
"""
def getDays(start_date, end_date, station):
    ds = pd.DataFrame()
    cur_start = datetime.strptime(start_date, "%Y%m%d")
    end_date = datetime.strptime(end_date, "%Y%m%d")
    cur_end = end_date
    
    while cur_start < end_date:
        #Calculates change
        remaining_days = (end_date - cur_start).days
        cur_end = cur_start + timedelta(days=min(30, remaining_days))

        #Calls API and takes in data
        load_dotenv()
        URL = f"https://api.weather.com/v2/pws/history/daily?stationId={station}&format=json&units=e&startDate={cur_start.strftime("%Y%m%d")}&endDate={cur_end.strftime("%Y%m%d")}&numericPrecision=decimal&apiKey={os.getenv('WEATHER_API_KEY')}"
        data = fetchBasicData(URL)
        if(data == None):
            return pd.DataFrame()        
        temp = pd.DataFrame(extractData(data))
        ds = pd.concat([ds, temp], ignore_index=True)
        cur_start = cur_end
    
    return pd.DataFrame(ds)

"""
Takes in multiple stations and gets infomation on a specific day

Returns a DataFrame
"""
def getDayofAllStations(date, stations):
    ds = pd.DataFrame()
    for st in stations:
        #print("Fetching data from station:", st)
        ds = pd.concat([ds, getDay(date, st)], ignore_index=True)
    return ds

"""
Takes in muliple stations and gets infomation from a timespan

Returns a DataFrame containing daily average for each station 
"""
def getDaysofAllStations(start_date, end_date, stations):
    ds = pd.DataFrame()
    for st in stations:
        print("Fetching data from station:", st)
        ds = pd.concat([ds, getDays(start_date, end_date, st)], ignore_index=True)
    return ds