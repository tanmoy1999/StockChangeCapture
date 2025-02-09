import requests
from datetime import datetime, timedelta
import pandas as pd
from datetime import datetime
import csv
import json
from io import StringIO
import requests
import os
import time
import random

def down(dt):

  URL = 'https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_' + str(dt) + '.csv'
  filename = 'test/sec_bhavdata_full_' + str(dt) + '.csv'
  session = requests.Session()
  session.headers = {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36",
      "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
      "Accept-Encoding": "gzip, deflate, br",
      "Accept-Language": "en-US,en;q=0.5",
      "Connection": "keep-alive"
  }

  try:
      response = session.get(URL, timeout=10)
      response.raise_for_status()  # Raise HTTPError for bad responses (4xx and 5xx)
      with open(filename, "wb") as file:
          file.write(response.content)
      return "File downloaded successfully"
  except requests.exceptions.RequestException as e:
      return e

os.makedirs("test")

# Start date: 1st January 2025
start_date = datetime(2024, 12, 1)

# End date: 31st January 2025
end_date = datetime(2024, 12, 31)

# Loop through each day in January 2025
current_date = start_date
while current_date <= end_date:
    # Check if the current day is not Saturday (5) or Sunday (6)
    if current_date.weekday() not in [5, 6]:
        # Format the date as DDMMYYYY
        formatted_date = current_date.strftime('%d%m%Y')
        print(formatted_date)
        print(down(formatted_date))
        # Move to the next day
    current_date += timedelta(days=1)
    # time.sleep(random.randint(1, 4))




def csv_to_json(csv_file):
    with open(csv_file, 'r') as file:
        csv_data = csv.reader(file)
        headers = next(csv_data)  # Get the header row
        json_data = {}

        for row in csv_data:
            node = row[2] + ' (' + row[1] +')'  # First column as the node
            children = {header: value for header, value in zip(headers[3:], row[3:])}  # Rest of the columns as children

            if node not in json_data:
                json_data[node] = []

            json_data[node].append(children)

    return json_data

try:
    path = 'test'
    dir_list = sorted(os.listdir(path))
    dir, dir1, dir2 = [], [], []
    for i in range(len(dir_list)):
        dt = dir_list[i].split('sec_bhavdata_full_')[1].split('.csv')[0]  # Extract '02012025'
        date_obj = datetime.strptime(dt, '%d%m%Y')  # Convert to datetime
        formatted_date = date_obj.strftime('%d%m%Y')  # Format as '02Jan2025'
        dir1.append(formatted_date)  # Append to list

    dir1 = sorted(dir1)

    for i in dir1:
        # d = dir1[i].strftime("%d%b%Y")
        fin = 'sec_bhavdata_full_'+str(i)+'.csv'
        dir2.append(fin)

    ticker = pd.read_csv('test/'+dir2[-1])[['SYMBOL']]
    url = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive"
    }
    response = requests.get(url, headers=headers)

    # Check if request was successful
    if response.status_code == 200:
        data = StringIO(response.text)  # Convert text to a file-like object
        ticker = pd.read_csv(data)
        ticker = ticker[ticker[' SERIES'] == "EQ"]
        ticker = ticker[['SYMBOL','NAME OF COMPANY']]
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")


    df2 = ticker.merge(ticker, on='SYMBOL', how='left')
    for i in dir_list:
        path = "test/"+str(i)
        print(path)
        prevClose = ' PREV_CLOSE' #PREV_CLOSE_'+str(i)
        close = ' CLOSE_PRICE'#'CLOSE_PRICE_'+str(i)
        df = pd.read_csv(path)
        df = df[df[' SERIES'] == " EQ"]
        df.rename(columns = {'PREV_CLOSE':prevClose, 'CLOSE_PRICE':close}, inplace = True)
        df[i] = ((df[close] - df[prevClose])/df[prevClose]) * 100
        df = df[["SYMBOL", i]]
        df = df.dropna()
        df2 = df2.merge(df, on='SYMBOL', how='left')
    df2 = df2.drop('NAME OF COMPANY_y', axis=1)
    df2.rename(columns = {'NAME OF COMPANY_x':'NAME OF COMPANY'}, inplace = True)


    #save file
    today = datetime.today()
    d4 = today.strftime('%b%Y')
    # d4 = 'Dec2024'
    filename = 'CSVOutput/' + str(d4) + '.csv'
    df2.to_csv(filename,index=False)
    print('Process completed... file generated ', filename)

    json_data = csv_to_json(filename)

    json_filename = 'JSONOutput/'+str(d4) + '.json'
    save_file = open(json_filename, "w")  
    json.dump(json_data, save_file, indent = 6)  
    save_file.close()  
    print(f"JSON file exported... {json_filename}")

    print(dir_list)
    dir_list_del = ['Test/'+ i for i in dir_list]
    for f in dir_list_del:
        os.remove(f)
    print("source files deleted...")
    os.rmdir("test")
except Exception as e:
    print(e)


