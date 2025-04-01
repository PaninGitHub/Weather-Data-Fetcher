#Importing the requests library
import requests
import pandas as pd
from functions import scrape
import time
import os

"""
This file is an example file of the module in use. Let's say we were trying to fetch daily averages from Houston throughout 2024.

First of all, we need the station ids. We can do this by going to the Wundermap website and getting the 
stations ids from there. 

Here's a map that covers most of Houston as well as some cities in Greater Houston
https://www.wunderground.com/wundermap?lat=29.78&lon=-95.38&zoom=12&radar=1&wxstn=0

To access the stations themshevles, we go to the settings on the right and click on 
the check box next to "Weather Stations". We then click on Temperature/Wind, which will 
show us the points represneting stations. If we click on an arbitary one, we will
we see the name of the station, as well as an id next to "Station ID". Copy the ID and
paste it in a tuple.

Lets take these stations for now:
{'KTXHOUST2924', 'KTXHOUST4614', 'KTXHOUST3627', 'KTXHOUST3784', 'KTXHOUST2719'}

Now, we know that the first and last day of 2024 was Jan 1st and Dec 31st.
However, we need to reformat this in YYYYMMDD format.

Jan 1st 2024 -> 20240101
Dec 31st 2024 -> 20241201

Now, we can scrape the data into a DataFrame
scrape.getDaysofAllStations(start day, end day, tuple of stations)
"""
tupleofStations =  {'KTXHOUST2924', 'KTXHOUST4614', 'KTXHOUST3627', 'KTXHOUST3784', 'KTXHOUST2719'}
ds = scrape.getDaysofAllStations('20240101', '20241201', tupleofStations)

# Specify the file path
# For the purposes of this example, we will set the file path to be the current working directory
current_dir = os.getcwd()
file_path = os.path.join(current_dir, "example.csv")
# Saving the DataFrame as CSV
ds.to_csv(file_path, index=False)

print(f"CSV file saved at {file_path}")