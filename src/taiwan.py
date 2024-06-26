import os
import sys
import argparse
import requests
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup



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


def main(date_range, output):  # Pass output directory as an argument
    
    for d in date_range:
        main_directory = f'{output}'
        url = f'https://www.tpex.org.tw/storage/bond_zone/tradeinfo/govbond//{d[:4]}/{d[:6]}/Curve.{d}-E.xls'
        filename = f"{main_directory}/Curve.{d}-E.xls"

        if not os.path.exists(main_directory):
            os.makedirs(main_directory)

        url = f'https://www.tpex.org.tw/storage/bond_zone/tradeinfo/govbond//{d[:4]}/{d[:6]}/Curve.{d}-E.xls'
        data = requests.get(url=url)
        
        if data.status_code == 200:
            filename = os.path.join(main_directory, f"Curve.{d}-E.xls")

            if not os.path.exists(filename):
                with open(filename, "wb") as file:
                    file.write(data.content)
                print(f"File downloaded successfully as '{filename}'")
            else:
                print(f"File '{filename}' already exists. Skipping download.")
        else:
            print(f"Failed to download file for date {d}. Status code: {data.status_code}")

        # for yield curve
       
        xls = pd.ExcelFile(filename)
        sheet_curve = xls.sheet_names[0]  # looking for curve sheet
        sheet_zero = xls.sheet_names[2]  # Looking for zero coupon curve sheet
        xls_curve = pd.read_excel(filename, sheet_curve)
        xls_zero = pd.read_excel(filename, sheet_zero)

        footer_rows = 1
        xls_curve = xls_curve.iloc[:-footer_rows]  # Drop the row with the note
        xls_curve = xls_curve.drop(["Bond Code", '剩餘年期                   (Residual Year)'], axis=1)  # Drop irrelevant columns
        xls_curve.set_index("Tenor", inplace=True)
        xls_curve = xls_curve.T
        xls_curve.reset_index(inplace=True)
        xls_curve.columns.name = None
        xls_curve.rename(columns={"index": ""}, inplace=True)
        xls_curve = xls_curve.drop([''], axis=1)

        new_col_name = {
            '2年(Year)': 'year_2',
            '5年(Year)': 'year_5',
            '10年(Year)': 'year_10',
            '20年(Year)': 'year_20',
            '30年(Year)': 'year_30',
        }

        xls_curve.rename(columns=new_col_name, inplace=True)
        xls_curve =  xls_curve.drop(['year_20','year_30'], axis = 1)


        output_directory = f'{main_directory}/{d[:4]}'
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)

        # Save the DataFrame to CSV file
        xls_curve.to_csv(f'{output_directory}/{d}.government_bonds_tw_yield_curve.csv.gz', index=False)
        

        # Deleting raw file
        if os.path.exists(filename):
            os.remove(filename)
            print(f"File '{filename}' deleted successfully.")
        else:
            print(f"File '{filename}' does not exist.")

if __name__ == '__main__':
    args = get_args()
    START_DATE = args.sdate
    END_DATE = args.edate
    OUTPUT_PATH = args.output
    date_range = generate_date_range(START_DATE, END_DATE)
    main(date_range, OUTPUT_PATH) 
