import pandas as pd
import requests
import feedparser
import re
from bs4 import BeautifulSoup
import itertools
import concurrent.futures


def get_est_info(pws):
    o_url = f"https://www.wunderground.com/dashboard/pws/{pws}/table/2022-11-23/2022-11-23/monthly"

    page_id = requests.get(o_url)
    bsoup = BeautifulSoup(page_id.content.decode('UTF-8'), "html.parser")
    results_info = bsoup.find_all("div", class_="sub-heading")[0]
    latitude = str(results_info).split("</strong>")[1].split(">")[1]
    if str(str(results_info).split("</strong>")[3].split("<")[0].split("°")[1]) == "W ":
        mlp = -1
    else:
        mlp = 1
    longitude = float(str(results_info).split("</strong>")[2].split(">")[1])* mlp
    height = round(float(str(results_info.find_all("strong")).split("</strong>")[0].split(" ")[2])/3.281)

    return latitude, longitude, height


def download_data(pws):
    dataframe = pd.DataFrame(columns= ["Date", "latitude", "longitude", "height", "tmax", "temp", "tmin", "dewpt", "hum_max", "hum_min", "Hi_wind", "wind", "pres_max", "pres_min", "daily_pcp"])

    def dates(start_yr, end_yr):
        dates_format = []
        for year in range(start_yr, end_yr+1):
            dates_format = dates_format + [f"{year}-{x}-10" for x in range(1, 13)]
        return dates_format
        
    
    dates_format = dates(2022, 2022)
    for date in dates_format:
        try:
            or_url = f"https://www.wunderground.com/dashboard/pws/{pws}/table/{date}/{date}/monthly" #YYYY-MM-DD

            get_data = pd.read_html(or_url, encoding="ISO-8859-15", decimal=".", thousands=",")

            table_raw = get_data[3]
            table_raw.columns = ["Date", "tmax", "temp", "tmin", "dewpt_max","dewpt", "dewpt_low", "hum_max", "hum", "hum_min", "Hi_wind", "wind", "low_wind", "pres_max", "pres_min", "daily_pcp"]

            month_table = table_raw.drop(columns=["hum", "dewpt_max", "dewpt_low", "low_wind"], axis=1)
            
            month_table['tmax'] = month_table['tmax'].apply(lambda x: round((float(x.split("\xa0")[0])-32)*5/9, 1))
            month_table['temp'] = month_table['temp'].apply(lambda x: round((float(x.split("\xa0")[0])-32)*5/9, 1))
            month_table['tmin'] = month_table['tmin'].apply(lambda x: round((float(x.split("\xa0")[0])-32)*5/9, 1))
            month_table['dewpt'] = month_table['dewpt'].apply(lambda x: round((float(x.split("\xa0")[0])-32)*5/9, 1))
            month_table['hum_max'] = month_table['hum_max'].apply(lambda x: x.split("\xa0")[0])
            month_table['hum_min'] = month_table['hum_min'].apply(lambda x: x.split("\xa0")[0])
            month_table['Hi_wind'] = month_table['Hi_wind'].apply(lambda x: round(float(x.split("\xa0")[0])*1.609, 1))
            month_table['wind'] = month_table['wind'].apply(lambda x: round(float(x.split("\xa0")[0])*1.609, 1))
            month_table['pres_max'] = month_table['pres_max'].apply(lambda x: round(float(x.split("\xa0")[0])*33.8637526, 1))
            month_table['pres_min'] = month_table['pres_min'].apply(lambda x: round(float(x.split("\xa0")[0])*33.8637526, 1))
            month_table['daily_pcp'] = month_table['daily_pcp'].apply(lambda x: round(float(x.split("\xa0")[0])*25.4, 1))

            frames = [dataframe, month_table]
            dataframe = pd.concat(frames)
            
            print(f"Station: {pws} completed at date {date}")
        except:
            pass
        
    geodata = get_est_info(pws)
    dataframe["latitude"] = geodata[0]
    dataframe["longitude"] = geodata[1]
    dataframe["height"] = geodata[2]
    
    dataframe.to_csv(f"{pws}.txt", sep="\t")

download_data("ITOBED1")

'''if __name__ == '__main__':    
    with concurrent.futures.ProcessPoolExecutor() as executor:
        f1 = [executor.submit(download_data, x) for x in pwsids]'''




