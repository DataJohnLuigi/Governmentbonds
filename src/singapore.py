import pandas as pd
import glob
import numpy as np
import requests
import os
import sys
from io import StringIO
from bs4 import BeautifulSoup as bs
from datetime import datetime, timedelta
import time

import argparse



# Note this script is for daily webscraping, back fill is not applicable
def get_args():
    arguments = argparse.ArgumentParser(
        description="Script to connect to a URL")

    arguments.add_argument(
        "-s",
        "--sdate",
        type=str,
        required=True,
        help="Start date"
    )

    arguments.add_argument(
        "-e",
        "--edate",
        type=str,
        required=True,
        help="End Date"
    )

    arguments.add_argument(
        "-o",
        "--output",
        type=str,
        required=True,
        help="Output directory"
    )

    return arguments.parse_args()


def generate_date_range(sdate, edate):
    start_date = datetime.strptime(sdate, '%Y%m%d')
    end_date = datetime.strptime(edate, '%Y%m%d')

    date_range = []

    current_date = start_date
    while current_date <= end_date:
        date_range.append(current_date.strftime('%Y%m%d'))
        current_date += timedelta(days=1)

    return date_range


def main(date_range, output,):  

#selecting the appropriate proxy for the environment
   

    
    for d in date_range:
        
        m = d[4:6]
        if m[0] == '0':
            m = m[1:]

        d1 = d[6:]
        if d1[0] == '0':
            d1 = d1[1:]


        main_directory = f'{output}'
        url = 'https://eservices.mas.gov.sg/statistics/fdanet/BenchmarkPricesAndYields.aspx'

        form_data = {
        '__EVENTTARGET':'', 
        '__EVENTARGUMENT':'', 
        '__LASTFOCUS':'', 
        '__VIEWSTATE': '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',
        '__VIEWSTATEGENERATOR': 'D112A42D',
        '__EVENTVALIDATION': '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',
        'ctl00$ContentPlaceHolder1$StartYearDropDownList': f'{d[:4]}',
        'ctl00$ContentPlaceHolder1$StartMonthDropDownList': f'{m}',
        'ctl00$ContentPlaceHolder1$EndYearDropDownList': f'{d[:4]}',
        'ctl00$ContentPlaceHolder1$EndMonthDropDownList': f'{m}',
        'ctl00$ContentPlaceHolder1$FrequencyDropDownList': 'D',
        'ctl00$ContentPlaceHolder1$DisplayButton': 'Display',
        'ctl00$ContentPlaceHolder1$SixMonthTreasuryBillYieldCheckBox': 'on',
        'ctl00$ContentPlaceHolder1$OneYearTreasuryBillYieldCheckBox': 'on',
        'ctl00$ContentPlaceHolder1$TwoYearBondYieldCheckBox': 'on',
        'ctl00$ContentPlaceHolder1$FiveYearBondYieldCheckBox': 'on',
        'ctl00$ContentPlaceHolder1$TenYearBondYieldCheckBox': 'on',
        }

        #DataFrame Processing
        print(f'Connecting to {url}...')
        response = requests.post(url=url, data=form_data)
        
        # time.sleep(5)
        
        if response.status_code == 200:
            print("Successfully Connected")
        
        else:
            print("Website Unreachable")
        
        # time.sleep(5)

        print("Processing Data ...")
        html_content =  bs(response.text,'html.parser')
        html_table = str(html_content.find('table', class_='results-table'))
        html_file = StringIO(html_table)
        df = pd.read_html(html_file)[0]
        # breakpoint()
        df['End of Period.2'] = df['End of Period.2'].astype(str)
        breakpoint()
        #this sections renames the columns of the dataframe
        df = df.rename(columns={'End of Period':'Year', 
                                'End of Period.1':'Month', 
                                'End of Period.2':'Day',
                                'AverageBuying Rates of Govt Securities Dealers 6-Month T-Bill Yield':'month_6',
                                'AverageBuying Rates of Govt Securities Dealers 1-Year T-Bill Yield':'year_1',
                                'AverageBuying Rates of Govt Securities Dealers 2-Year Bond Yield':'year_2',
                                'AverageBuying Rates of Govt Securities Dealers 5-Year Bond Yield':'year_5',
                                'AverageBuying Rates of Govt Securities Dealers 10-Year Bond Yield':'year_10',
                            })
        
        #Filtering the dataframe based on the date
        df2 = df.query(f"Day == '{d1}'")
        df2 = df2.drop(['Year', 'Month', 'Day'], axis=1)
        # Reset the index
        df2 = df2.reset_index(drop=True)

        download_dir = f'{output}/{d[:4]}'

        if not os.path.exists(download_dir):
            os.makedirs(download_dir)

        file_name = f'{download_dir}/{d}.government_bond_yields_sg.csv.gz'

        #Saving to CSV
        time.sleep(3)
        print(f"Downloading {d}.government_bond_yields_sg.csv.gz ...")
        df2.to_csv(file_name, index=False)

    time.sleep(5)        
    print(f'Successfully Downloaded {d}.government_bond_yields_sg.csv.gz')

if __name__ == '__main__':
    args = get_args()
    
    START_DATE = args.sdate
    END_DATE = args.edate
    OUTPUT_PATH = args.output
    date_range = generate_date_range(START_DATE, END_DATE)  
    main(date_range, OUTPUT_PATH,)