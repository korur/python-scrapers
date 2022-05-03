# -*- coding: utf-8 -*-
"""
Created on Wed Aug 26 08:39:22 2020

@author: serda
"""

#%% Scraping Downloadspeeds data
from bs4 import BeautifulSoup
import requests
import pandas as pd
import time
import re
import datetime as dt
from datetime import timedelta
import os

#%% Initialize the data container
from dataclasses import dataclass, field
import pathlib
path = pathlib.Path().cwd().parent

from pushover import init, Client
po_token = 'a15ggw644m446oizdvew9iecvyr2d4'
po_user = 'uh918iwuy7nbtaz6chgsg6nh15awin'

@dataclass(frozen=True)
class Countries:
    country_pop = pd.read_excel(path / "country_pop_62.xlsx")
    name: list = field(default = tuple(country_pop.country))
    speedtest_names: list = field(default=tuple(country_pop.speedtest_names))
    code_2_letter: list = field(default = tuple(country_pop.country_code))
    code_3_letter: list = field(default = tuple(country_pop.country_code_3))
    pop: list = field(default = tuple(country_pop.population))
    date_format: str = field(default = "%Y-%m-%d")

# Initiate the dataclass
countries = Countries()

target_url = "https://www.speedtest.net/global-index"

def check_there_is_new_data_and_run(folder='download_speeds'):
    stored = path / folder
    os.chdir(stored)
    latest_data = os.listdir()
    latest_data.sort(key=os.path.getctime)
    testdf = pd.read_excel(latest_data[-1])
    last_saved = testdf.columns[-1]
    if type(last_saved) == str:
        last_saved = dt.datetime.strptime(last_saved, countries.date_format)
    soup = BeautifulSoup(requests.get(target_url).text, 'lxml')
    soup_current = soup.find('div', {'class', 'month'}).text
    available_data = dt.datetime.strptime(soup_current, '%B %Y')
    time_diff = available_data - last_saved
    if time_diff > timedelta(days=1):
        downloadspeed = get_downloadspeeds_df(target_url, soup, stored)
        return downloadspeed
    else:
        init(po_token)
        Client(po_user).send_message("There is no new data on speedtest, script not run", title="Downloadspeeds")




def get_downloadspeeds_df(target_url, soup, stored):
    base = re.match('^.+?[^\/:](?=[?\/]|$)', target_url)[0]
    countries_soup = soup.find_all('td', {'class': 'country'})
    print("list of countries collected")
    print('processing each country')
    country_list = list(countries.speedtest_names)
    country_urls = []
    for i in countries_soup:
        d = {}
        d['country'] = i.text.strip()
        d['url'] = base + i.find('a').get('href')
        if d['country'] in country_list:
            country_urls.append(d)
        else:
            continue
    country_urls = [c for c in country_urls if  c['url'][-5:] == 'fixed' ]
    for b in country_urls:
        print('processing: ', b)
        url = b['url']
        soup = BeautifulSoup(requests.get(url).text, 'lxml')
        time.sleep(3)
        data = soup.find_all('script',{'type':'text/javascript'})[1].string.split("download_mbps",)[1:14]
        data2 = []
        for i in data:
            score = i.split(",")[0]
            data2.append(score)
        data3 = []
        for i in data2:
            score = i.split(":")[1].strip()
            data3.append(score)
        data4 = []
        for i in data2:
            score = i.split('"')[2]
            data4.append(score)
        print(data4)
        b['Date1'] = data4[0]
        b['Date2'] = data4[1]
        b['Date3'] = data4[2]
        b['Date4'] = data4[3]
        b['Date5'] = data4[4]
        b['Date6'] = data4[5]
        b['Date7'] = data4[6]
        b['Date8'] = data4[7]
        b['Date9'] = data4[8]
        b['Date10'] = data4[9]
        b['Date11'] = data4[10]
        b['Date12'] = data4[11]
        b['Date13'] = data4[12]
    # Create a data frame from the collected information
    downloadspeed = pd.DataFrame(country_urls)
    downloadspeed = downloadspeed.rename(columns={'country': 'speedtest_names'})
    missing = set(countries.speedtest_names) - set(downloadspeed.speedtest_names.to_list())
    if len(missing) > 0:
        print('following countries were missing in the data:')
        print("\t", missing)
    germany_url = base + '/global-index/germany#fixed'
    soup = BeautifulSoup(requests.get(germany_url).text, 'lxml')
    time.sleep(3)
    latest = soup.find_all('script', {'type': 'text/javascript'})[1]
    latest = latest.string.split("download_mbps", )[13].split('"month":"')[1][0:7]
    latest_date = latest + '-01'
    first = soup.find_all('script', {'type': 'text/javascript'})[1]
    first = first.string.split("download_mbps", )[1].split('"month":"')[1][0:7]
    first_date = first + '-01'

    ### find the dates of every month for the past year
    past_year = pd.date_range(first_date, latest_date,
                              freq='MS').strftime("%Y-%m-%d").tolist()
    col_list = ['country', 'url']
    col_list.extend(past_year)
    col_list
    downloadspeed.columns = col_list
    downloadspeed = downloadspeed.rename(columns={'country': 'speedtest_names'})
    dx = pd.DataFrame(list(zip(countries.speedtest_names, countries.name)), \
                      columns=['speedtest_names', 'country'])
    joined = pd.merge(downloadspeed, dx, how='right')
    del joined['speedtest_names']
    joined.set_index('country', inplace=True)
    joined.sort_index(inplace=True)
    del joined['url']

    assert joined.index.to_list() == list(countries.name)

    writer = pd.ExcelWriter( stored / (dt.datetime.now().strftime("%Y_%m_%d-%I_%M_%S_%p") + '_downloadspeeds.xlsx'),
                            engine='xlsxwriter')
    joined.to_excel(writer, sheet_name='main_data', index=True)
    writer.save()

    # open the worksheet (googlesheet)
    import gspread_pandas
    from gspread_pandas import Spread
    myconfig = gspread_pandas.conf.get_config(conf_dir='../', file_name='google_secret.json')
    spread = Spread('time_series_data_all_indicators_drive', config=myconfig)  # this uses gspread_pandas package
    # Write on the order_rank sheet of the 'time_series_data_all_indicators_drive' googlesheet
    spread.df_to_sheet(joined, index=True, sheet='infra_download_speed', start='A1', replace=True)
    # pushover messages
    init(po_token)
    Client(po_user).send_message("Script succesfully run!", title="Downloadspeeds")
    return joined


# Run the script
try:
    check_there_is_new_data_and_run(folder='download_speeds')
except:
    init(po_token)
    Client(po_user).send_message("An error occured!", title="Downloadspeeds")















