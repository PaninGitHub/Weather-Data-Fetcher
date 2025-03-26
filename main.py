#Importing the requests library
import requests
import pandas as pd
from functions import scrape
import time


URL = "https://api.weather.com/v2/pws/history/all?stationId=KTXPASAD198&format=json&units=e&date=20241229&numericPrecision=decimal&apiKey=e1f10a1e78da46f5b10a1e78da96f525"
stations = {'KTXHOUST2924', 'KTXHEMPS30', 'KTXHOUST4614', 'KTXHOUST3627', 'KTXANGLE175', 'KTXTEXAS1184', 'KTXSUGAR311', 'KTXANGLE54'}

#ds = scrape.getDayofAllStations('20241230', stations)
start_time = time.time()
ds = scrape.getDaysofAllStations('20220101', '20241130', stations)
end_time = time.time()
elapsed_time = end_time - start_time
print(f"The code took {elapsed_time} seconds to run.")
print(ds)