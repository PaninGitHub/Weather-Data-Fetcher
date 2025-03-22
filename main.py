#Importing the requests library
import requests
import pandas as pd
from functions import scrape

URL = "https://api.weather.com/v2/pws/history/all?stationId=KTXPASAD198&format=json&units=e&date=20241229&numericPrecision=decimal&apiKey=e1f10a1e78da46f5b10a1e78da96f525"
stations = {'KTXHOUST2924', 'KTXHEMPS30', 'KTXHOUST4614', 'KTXHOUST3627', 'KTXANGLE175', 'KTXTEXAS1184', 'KTXSUGAR311', 'KTXANGLE54'}

#ds = scrape.getDayofAllStations('20241230', stations)
ds = scrape.getDays('20241101', '20241130', 'KTXBELLA15')
ds = scrape.getDaysofAllStations('20241101', '20241130', stations)
print(ds)