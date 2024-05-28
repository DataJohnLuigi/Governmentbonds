# import os
# import sys
# import xlrd
# import json
# from bs4 import BeautifulSoup as bs
# import requests
# import argparse

# import pandas as pd
# from datetime import datetime
# from datetime import datetime, timedelta




# # Functions

# def get_args():
#     arguments = argparse.ArgumentParser(
#         description="Script to connect to a URL")

#     arguments.add_argument(
#         "-s",
#         "--sdate",
#         type=str,
#         required=True,
#         help="Start date"
#     )

#     arguments.add_argument(
#         "-e",
#         "--edate",
#         type=str,
#         required=True,
#         help="End Date"
#     )

#     arguments.add_argument(
#         "-o",
#         "--output",
#         type=str,
#         required=True,
#         help="Output directory"
#     )

#     return arguments.parse_args()


# def generate_date_range(sdate, edate):
#     start_date = datetime.strptime(sdate, '%Y%m%d')
#     end_date = datetime.strptime(edate, '%Y%m%d')

#     date_range = []

#     current_date = start_date
#     while current_date <= end_date:
#         date_range.append(current_date.strftime('%Y%m%d'))
#         current_date += timedelta(days=1)

#     return date_range

# def main(date_range,output):    
    


#     for d in date_range:
#         sdate_obj = datetime.strptime(d, '%Y%m%d')
#         edate_obj = datetime.strptime(d, '%Y%m%d')
        
#         startDate=sdate_obj.strftime('%Y-%m-%d')
#         endDate=edate_obj.strftime('%Y-%m-%d')

#         main_directory = f'{output}'
#         filename = f"{main_directory}/Curve.{d}-E.xls"

#         url = f'https://yield.chinabond.com.cn/cbweb-mn/yc/downBzqxDetail?ycDefIds=2c9081e50a2f9606010a3068cae70001&&zblx=txy&&workTime={startDate}&&dxbj=0&&qxlx=0&&yqqxN=N&&yqqxK=K&&wrjxCBFlag=0&&locale=zh_CN'
#         data = requests.get(url=url, 
#                            )

#         if data.status_code == 200:
#             filename = os.path.join(main_directory, f"Curve.{d}-E.xls")
#             os.makedirs(main_directory, exist_ok=True)

#             if not os.path.exists(filename):
#                 with open(filename, "wb") as file:
#                     file.write(data.content)
#                 print(f"File downloaded successfully as '{filename}'")
#             else:
#                 print(f"File '{filename}' already exists. Skipping download.")
#         else:
#             print(f"Failed to download file for date {d}. Status code: {data.status_code}")

#         # Data Processing Section
#         df = pd.read_excel(filename)
#         df = df.T
#         df.reset_index(inplace=True, drop=True)
#         df = df.drop(0)
#         df.reset_index(inplace=True, drop=True)
#         df = df.drop([0, 1, 2, 3, 5, 8, 10, 12, 13, 14, 15, 16], axis=1)
#         df.columns = df.iloc[0]
#         df = df.drop(0)
#         df.columns = df.columns.astype(str)
#         df.rename(columns={"0.5": "month_6", '1.0': 'year_1', '2.0': 'year_2', '5.0': 'year_5', '10.0': 'year_10'}, inplace=True)
#         output_directory = f'{main_directory}/{d[:4]}'
#         if not os.path.exists(output_directory):
#             os.makedirs(output_directory)

#         # Save the DataFrame to CSV file
#         df.to_csv(f'{output_directory}/{d}.government_bonds_cn.csv.gz', index=False)

#         if os.path.exists(filename):
#             os.remove(filename)
#             print(f"File '{filename}' deleted successfully.")
#         else:
#             print(f"File '{filename}' does not exist.")

# if __name__ == '__main__':
#     args = get_args()
#     START_DATE = args.sdate
#     END_DATE = args.edate
#     OUTPUT_PATH = args.output
#     # ENVIRON = args.environ
#     date_range = generate_date_range(START_DATE, END_DATE)  # Save the date range
#     main(date_range, OUTPUT_PATH)  # Pass date range and output path to main function


import os
import sys
import json
import requests
import argparse
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup as bs

def get_args():
    arguments = argparse.ArgumentParser(
        description="Script to download and process bond data"
    )

    arguments.add_argument(
        "-s",
        "--sdate",
        type=str,
        required=True,
        help="Start date in YYYYMMDD format"
    )

    arguments.add_argument(
        "-e",
        "--edate",
        type=str,
        required=True,
        help="End date in YYYYMMDD format"
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
    print("date generating")
    start_date = datetime.strptime(sdate, '%Y%m%d')
    end_date = datetime.strptime(edate, '%Y%m%d')

    date_range = []
    current_date = start_date
    while current_date <= end_date:
        date_range.append(current_date.strftime('%Y%m%d'))
        current_date += timedelta(days=1)

    return date_range

def main(date_range, output):
    print("Connecting to website")
    for d in date_range:
        sdate_obj = datetime.strptime(d, '%Y%m%d')
        start_date = sdate_obj.strftime('%Y-%m-%d')
        main_directory = output
        filename = f"{main_directory}/Curve.{d}-E.xls"

        url = f'https://yield.chinabond.com.cn/cbweb-mn/yc/downBzqxDetail?ycDefIds=2c9081e50a2f9606010a3068cae70001&&zblx=txy&&workTime={start_date}&&dxbj=0&&qxlx=0&&yqqxN=N&&yqqxK=K&&wrjxCBFlag=0&&locale=zh_CN'
        response = requests.get(url)

        if response.status_code == 200:
            os.makedirs(main_directory, exist_ok=True)

            if not os.path.exists(filename):
                with open(filename, "wb") as file:
                    file.write(response.content)
                print(f"File downloaded successfully as '{filename}'")
            else:
                print(f"File '{filename}' already exists. Skipping download.")
        else:
            print(f"Failed to download file for date {d}. Status code: {response.status_code}")
            continue

        # Data Processing Section
        try:
            df = pd.read_excel(filename)
            df = df.T.reset_index(drop=True).drop(0).reset_index(drop=True)
            df = df.drop([0, 1, 2, 3, 5, 8, 10, 12, 13, 14, 15, 16], axis=1)
            df.columns = df.iloc[0]
            df = df.drop(0)
            df.columns = df.columns.astype(str)
            df.rename(columns={"0.5": "month_6", '1.0': 'year_1', '2.0': 'year_2', '5.0': 'year_5', '10.0': 'year_10'}, inplace=True)
            output_directory = os.path.join(main_directory, d[:4])
            os.makedirs(output_directory, exist_ok=True)
            
            # Save the DataFrame to a CSV file
            df.to_csv(f'{output_directory}/{d}.government_bonds_cn.csv.gz', index=False)

            if os.path.exists(filename):
                os.remove(filename)
                print(f"File '{filename}' deleted successfully.")
            else:
                print(f"File '{filename}' does not exist.")
        except Exception as e:
            print(f"Error processing file for date {d}: {e}")

if __name__ == '__main__':
    args = get_args()
    START_DATE = args.sdate
    END_DATE = args.edate
    OUTPUT_PATH = args.output

    date_range = generate_date_range(START_DATE, END_DATE)
    main(date_range, OUTPUT_PATH)
