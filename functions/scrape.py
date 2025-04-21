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


"""
Takes in JSON object data of a weather station fetched from the API and converts
each observation/feature into an object.

Returns a list of newly formatted observations of hourly data.
"""
def extractHourlyData(_data):
    temp = []
    for observation in _data['observations']:
        # Extract and append the necessary properties
        observation_data = {
            "stationID": observation["obs_id"],
            "obsTimeUtc": datetime.fromtimestamp(observation["valid_time_gmt"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z"),
            "temperature": observation["temp"],
            "weather": observation["wx_phrase"],
            "windspeed": observation["wspd"],
            "pressure": observation["pressure"]
        }
        temp.append(observation_data)
    return temp



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

async def getDayAsync(date, station):
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
    #Gets dates
    ds = pd.DataFrame()
    cur_start = datetime.strptime(start_date, "%Y%m%d")
    end_date = datetime.strptime(end_date, "%Y%m%d")
    cur_end = end_date
    load_dotenv()

    while cur_start < end_date:
        #Calculates change
        remaining_days = (end_date - cur_start).days
        cur_end = cur_start + timedelta(days=min(30, remaining_days))

        #Calls API and takes in data
        URL = f"https://api.weather.com/v2/pws/history/daily?stationId={station}&format=json&units=e&startDate={cur_start.strftime("%Y%m%d")}&endDate={cur_end.strftime("%Y%m%d")}&numericPrecision=decimal&apiKey={os.getenv('WEATHER_API_KEY')}"
        data = fetchBasicData(URL)
        if(data == None):
            return pd.DataFrame()        
        temp = pd.DataFrame(extractData(data))
        ds = pd.concat([ds, temp], ignore_index=True)
        cur_start = cur_end
    
    return pd.DataFrame(ds)

def getDaysAsync(start_date, end_date, station):
    #Gets URLs for dates
    allURLs = buildDailyWeatherURLs(start_date, end_date, station)

    #Gathers a list of responses from async API calls to all URLs
    res_list = asyncio.run(gatherBasicDataAsync(allURLs))

    #Processes into a DataFrame
    ds = processMultipleBasicData(res_list)
    #Returns DataFrame, which contains all days n stuff
    return pd.DataFrame(ds)


"""
Gets hourly infomation on a station on a section of time using the same method.
Note that the stations used for daily weather data is different than for hourly weather data.

Example of what a station should look like:
KCLL:9:US
"""
def getHoursofDays(start_date, end_date, station):
    #Gets dates
    ds = pd.DataFrame()
    cur_start = datetime.strptime(start_date, "%Y%m%d")
    end_date = datetime.strptime(end_date, "%Y%m%d")
    cur_end = end_date
    load_dotenv()

    while cur_start < end_date:
        #Calculates change
        remaining_days = (end_date - cur_start).days
        cur_end = cur_start + timedelta(days=min(30, remaining_days))

        #Calls API and takes in data
        URL = f"https://api.weather.com/v1/location/{station}/observations/historical.json?apiKey={os.getenv('WEATHER_API_KEY')}&units=e&startDate={cur_start.strftime("%Y%m%d")}&endDate={cur_end.strftime("%Y%m%d")}"
        data = fetchBasicData(URL)
        if(data == None):
            return pd.DataFrame()        
        temp = pd.DataFrame(extractHourlyData(data))
        ds = pd.concat([ds, temp], ignore_index=True)
        cur_start = cur_end
    
    return pd.DataFrame(ds)

def getHoursofDaysAsync(start_date, end_date, station):
    #Gets URLs for dates
    allURLs = buildHourlyDataWeatherURLs(start_date, end_date, station)

    #Gathers a list of responses from async API calls to all URLs
    res_list = asyncio.run(gatherBasicDataAsync(allURLs))
    #Processes into a DataFrame
    ds = processMultipleHourlyBasicData(res_list)
    #Returns DataFrame, which contains all days n stuff
    return pd.DataFrame(ds)


"""
Takes in a dateframe and returns a list of needed API requests to fetch all
data for a specific station

Used for async fetching of data
"""
def buildDailyWeatherURLs(start_date, end_date, station):
    #Gets dates
    ds = pd.DataFrame()
    cur_start = datetime.strptime(start_date, "%Y%m%d")
    end_date = datetime.strptime(end_date, "%Y%m%d")
    cur_end = end_date
    load_dotenv()

    #Fetch URLs
    allURLs = []
    while cur_start < end_date:
        #Calculates change
        remaining_days = (end_date - cur_start).days
        cur_end = cur_start + timedelta(days=min(30, remaining_days))

        #Fetches URL
        URL = f"https://api.weather.com/v2/pws/history/daily?stationId={station}&format=json&units=e&startDate={cur_start.strftime("%Y%m%d")}&endDate={cur_end.strftime("%Y%m%d")}&numericPrecision=decimal&apiKey={os.getenv('WEATHER_API_KEY')}"
        allURLs.append(URL)

        #Increments cur_start appropirately      
        cur_start = cur_end
    
    #Return list of URLs
    return allURLs

"""
Do the same but for hourly data
"""
def buildHourlyDataWeatherURLs(start_date, end_date, station):
    #Gets dates
    ds = pd.DataFrame()
    cur_start = datetime.strptime(start_date, "%Y%m%d")
    end_date = datetime.strptime(end_date, "%Y%m%d")
    cur_end = end_date
    load_dotenv()

    #Fetch URLs
    allURLs = []
    while cur_start < end_date:
        #Calculates change
        remaining_days = (end_date - cur_start).days
        cur_end = cur_start + timedelta(days=min(30, remaining_days))

        #Fetches URL
        URL = f"https://api.weather.com/v1/location/{station}/observations/historical.json?apiKey={os.getenv('WEATHER_API_KEY')}&units=e&startDate={cur_start.strftime("%Y%m%d")}&endDate={cur_end.strftime("%Y%m%d")}"
        allURLs.append(URL)

        #Increments cur_start appropirately      
        cur_start = cur_end
    
    #Return list of URLs
    return allURLs


"""
Takes in a list of URLs and converts it into a list of 
JSON objects that represent API requests

Reference: https://youtu.be/Ii7x4mpIhIs?si=Iwo5PImjAQliivKa&t=169
"""
async def gatherBasicDataAsync(urls):
    #Take in tasks
    tasks = []
    async with aiohttp.ClientSession() as session:
        for url in urls:
            task = asyncio.create_task(fetchBasicDataAsync(url, session))
            tasks.append(task)
        res_list = await asyncio.gather(*tasks)
        return res_list

"""
Takes in a list of responses and processes each one, then combines them in sorted order

Meant to take in response from gatherBasicDataAsync
"""
def processMultipleBasicData(reslist):
    df_list = []
    for r in reslist:
        df_list.append(pd.DataFrame(extractData(r)))
    df = pd.concat(df_list)
    
    #Converts to datetime to then sort
    df["obsTimeUtc"] = pd.to_datetime(df["obsTimeUtc"], utc=True)
    df = df.sort_values(by='obsTimeUtc')

    #Might need to implement grouping
    return df

"""
Does the same but for hourly data
"""
def processMultipleHourlyBasicData(reslist):
    df_list = []
    for r in reslist:
        df_list.append(pd.DataFrame(extractHourlyData(r)))
    df = pd.concat(df_list)
    #Converts to datetime to then sort
    df["obsTimeUtc"] = pd.to_datetime(df["obsTimeUtc"], utc=True)
    df = df.sort_values(by='obsTimeUtc')

    #Might need to implement grouping
    return df

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
    print("======STARTING PROCESS OF COLLETING WEATHER DATA======")
    ds = pd.DataFrame()
    for st in stations:
        start_time = time.time()
        ds = pd.concat([ds, getDays(start_date, end_date, st)], ignore_index=True)
        end_time = time.time()
        elapsed_time = end_time - start_time
        print("Fetched data from", st, "in", elapsed_time, "seconds")
    print("======FINISHED PROCESS OF COLLETING WEATHER DATA======")
    return ds

"""
Does the same but for hourly data
"""
def getHourlyDataofAllStations(start_date, end_date, stations):
    print("======STARTING PROCESS OF COLLETING WEATHER DATA======")
    ds = pd.DataFrame()
    for st in stations:
        start_time = time.time()
        ds = pd.concat([ds, getHoursofDays(start_date, end_date, st)], ignore_index=True)
        end_time = time.time()
        elapsed_time = end_time - start_time
        print("Fetched data from", st, "in", elapsed_time, "seconds")
    print("======FINISHED PROCESS OF COLLETING WEATHER DATA======")
    return ds


"""
Takes in muliple stations and gets infomation from a timespan async

Returns a DataFrame containing daily average for each station 
"""
def getDaysofAllStationsAsync(start_date, end_date, stations):
    print("======STARTING PROCESS OF COLLETING WEATHER DATA======")
    ds = pd.DataFrame()
    for st in stations:
        start_time = time.time()
        ds = pd.concat([ds, getDaysAsync(start_date, end_date, st)], ignore_index=True)
        end_time = time.time()
        elapsed_time = end_time - start_time
        print("Fetched data from", st, "in", elapsed_time, "seconds")
    print("======FINISHED PROCESS OF COLLETING WEATHER DATA======")
    return ds

"""
Does the same but for hourly data
"""
def getHourlyDataofAllStationsAsync(start_date, end_date, stations):
    print("======STARTING PROCESS OF COLLETING WEATHER DATA======")
    ds = pd.DataFrame()
    for st in stations:
        if(st == ""): continue
        start_time = time.time()
        ds = pd.concat([ds, getHoursofDaysAsync(start_date, end_date, st)], ignore_index=True)
        end_time = time.time()
        elapsed_time = end_time - start_time
        print("Fetched data from", st, "in", elapsed_time, "seconds")
    print("======FINISHED PROCESS OF COLLETING WEATHER DATA======")
    return ds