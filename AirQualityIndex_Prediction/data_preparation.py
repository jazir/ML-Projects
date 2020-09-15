""" File name: data_preparation
    Aims: 1) Download climate web pages from website
          2) Scrape the required data from the downloaded web pages"""

import os
import sys
import requests
import pandas as pd


def download_data_tutiempo(start_year, end_year):
    """Download climate data of Oklahoma City (USA) from https://en.tutiempo.net and save them year-wise """

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            if month < 10:  # This is to give the leading zero value. Also, it will help to keep the month order.
                month_val = str(month).zfill(2)
                url = f'https://en.tutiempo.net/climate/{month_val}-{year}/ws-723530.html'
            else:
                month_val = month
                url = f'https://en.tutiempo.net/climate/{month_val}-{year}/ws-723530.html'

            texts = requests.get(url)
            text_utf = texts.text.encode('utf=8')

            if not os.path.exists(f"Data/tutiempo_Data/{year}"):
                os.makedirs(f"Data/tutiempo_Data/{year}")
            with open(f"Data/tutiempo_Data/{year}/{month_val}.html", "wb") as output:
                output.write(text_utf)

            sys.stdout.flush()


def download_data_okc_gov(start_year, end_year):
    """Download climate data of Oklahoma City (USA) from https://applications.deq.ok.gov and save them year-wise """

    for year in range(start_year, end_year + 1):
        url = f'https://applications.deq.ok.gov/webdata/aqd/monitoring/charts/OKCAQI{year}.htm'
        texts = requests.get(url)
        text_utf = texts.text.encode('utf=8')

        if not os.path.exists(f"Data/OKCgov_Data"):
            os.makedirs(f"Data/OKCgov_Data")
        with open(f"Data/OKCgov_Data/{year}.html", "wb") as output:
            output.write(text_utf)

        sys.stdout.flush()


def download_climate_data(start_year, end_year):
    """Download climate data from websites"""

    download_data_tutiempo(start_year, end_year)
    download_data_okc_gov(start_year, end_year)


def tutiempo_met_data():
    """Extract the required data from downloaded web pages of https://en.tutiempo.net
    and convert them into pandas dataframe"""

    list = []
    for subdir, dirs, files in os.walk('Data/tutiempo_Data'):
        print('{} {} {}'.format(repr(subdir), repr(dirs), repr(files)))

        for filename in files:
            filepath = subdir + os.sep + filename
            if filepath.endswith(".html"):
                list.append(filepath)

    df = pd.DataFrame(columns=('T', 'TM', 'Tm', 'SLP', 'H', 'PP', 'VV', 'V', 'VM'))

    for path in list:
        table1 = pd.read_html(path, header=None)
        table1 = table1[2]
        table1 = table1.iloc[:-2].drop(['Day', 'VG', 'RA', 'SN', 'TS', 'FG'], axis=1)

        df = pd.concat([df, table1], ignore_index=True)

    return df


def okc_met_data():
    """Extract the required data from downloaded web pages of https://applications.deq.ok.gov
    and convert them into pandas dataframe"""

    list = []
    for subdir, dirs, files in os.walk('Data/OKCgov_Data'):
        print('{} {} {}'.format(repr(subdir), repr(dirs), repr(files)))

        for filename in files:
            filepath = subdir + os.sep + filename
            if filepath.endswith(".html"):
                list.append(filepath)

    cols = ['Date', 'N1', 'Ozone conc', 'Ozone AQI', 'N2', 'PM-2.5 conc', 'PM-2.5 AQI',
            'N3', 'NO2 conc', 'NO2 AQI', 'N4', 'SO2 conc', 'SO2 AQI', 'N5',
            'CO conc', 'CO AQI', 'N6', 'PM-10 conc', 'PM-10 AQI']  # Nx corresponds to the null columns
    df = pd.DataFrame(columns=cols)

    for path in list:
        table1 = pd.read_html(path, header=None)
        table1 = table1[0]
        table1.columns = cols
        table1 = table1.iloc[3:]
        table1 = table1.iloc[:-1]
        df = pd.concat([df, table1], ignore_index=True)

    # Changing the all the numeric columns to integers. Then only we can calculate the max value
    df[cols[1:]] = df[cols[1:]].apply(pd.to_numeric, errors='coerce')
    df = df.drop(['N1', 'N2', 'N3', 'N4', 'N5', 'N6'], axis=1)
    df['AQI'] = df[['Ozone AQI', 'PM-2.5 AQI', 'NO2 AQI', 'SO2 AQI', 'CO AQI', 'PM-10 AQI']].max(axis=1)
    df = df.drop(['Ozone AQI', 'PM-2.5 AQI', 'NO2 AQI', 'SO2 AQI', 'CO AQI', 'PM-10 AQI'], axis=1)
    return df


def extract_and_merge_data():
    """ Extract the required climate tables from downloaded websites
     and create the dataset in .csv format"""
    df1 = tutiempo_met_data()
    df2 = okc_met_data()
    df = pd.concat([df1, df2], axis=1)
    col_name = 'Date'  # To set date as the first column
    first_column = df.pop(col_name)
    df.insert(0, col_name, first_column)

    df.to_csv('AQIdata.csv', index=False)


if __name__ == "__main__":
    start_year = 2017
    end_year = 2019

    download_climate_data(start_year, end_year)
    extract_and_merge_data()
