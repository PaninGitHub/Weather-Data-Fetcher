#Importing the requests library
import requests
import pandas as pd
from functions import scrape
import time


URL = "https://api.weather.com/v2/pws/history/all?stationId=KTXPASAD198&format=json&units=e&date=20241229&numericPrecision=decimal&apiKey=e1f10a1e78da46f5b10a1e78da96f525"
stations = {'KTXHOUST2924', 'KTXHOUST4614', 'KTXHOUST3627', 'KTXHOUST3784', 'KTXHOUST2719', 'KTXHOUST3729', 'KTXHOUST3329', 'KTXHOUST4201', 'KTXHOUST3206', 'KTXHOUST4140', 'KTXHOUST4859', 'KTXHOUST3065', 'KTXHOUST4942', 'KTXHOUST2511', 'KTXHOUST3870', 'KTXHOUST4924', 'KTXHOUST4257', 'KTXHOUST2472', 'KTXHOUST2512', 'KTXHOUST600', 'KTXHOUST4198', 'KTXHOUST2589', 'KTXHOUST3192', 'KTXHOUST3257', 'KTXHOUST3746', 'KTXHOUST3338', 'KTXHOUST2780', 'KTXHOUST2579', 'KTXHOUST4882', 'KTXHOUST4515', 'KTXHOUST4025', 'KTXHOUST4794', 'KTXHOUST3883', 'KTXHOUST3665', 'KTXHOUST3578', 'KTXHOUST3089', 'KTXHOUST4791', 'KTXHOUST4002', 'KTXHOUST3728', 'KTXHOUST3212', 'KTXHOUST2835', 'KTXHOUST4892', 'KTXHOUST4125', 'KTXHOUST394', 'KTXHOUST4108', 'KTXHOUST3045', 'KTXHOUST4766', 'KTXHOUST4256', 'KTXHOUST4857', 'KTXHOUST3998', 'KTXHOUST3738', 'KTXHOUST1998', 'KTXHOUST2978', 'KTXHOUST3919', 'KTXHOUST2226', 'KTXHOUST2659', 'KTXHOUST3561', 'KTXHOUST3787', 'KTXHOUST4806', 'KTXHOUST4523', 'KTXHOUST4726', 'KTXHOUST4874'}

#ds = scrape.getDayofAllStations('20241230', stations)
start_time = time.time()
ds = scrape.getDaysofAllStations('20250101', '20250324', stations)
end_time = time.time()
elapsed_time = end_time - start_time
print(f"The code took {elapsed_time} seconds to run.")
print(ds)

# Specify the file path
file_path = r'C:\Users\dinid\OneDrive\[1] Work\[0] Texas A&M\[2] ATMO\output.csv'

# Saving the DataFrame as CSV
ds.to_csv(file_path, index=False)

print(f"CSV file saved at {file_path}")