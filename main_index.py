import os
import pandas as pd
import numpy as np
import wget
import zipfile
from datetime import datetime, date
import requests, zipfile, io
import json
import csv


def multi_node_export(csv_files):
  json_data = {}
  for csv_file in csv_files:
      with open(csv_file, "r") as file:
          csv_reader = csv.reader(file)
          header = next(csv_reader)
          main_node = csv_file.replace(".csv", "").split('/')[1]

          json_data[main_node] = []

          for row in csv_reader:
              sub_node = row[0]
              data = {
                  header[i]: row[i] for i in [6,7]
              }

              json_data[main_node].append({sub_node: data})

  return json_data

def msg(message):
    print(message)
    
    bot_token = '5041715929:AAFcraPI9-8jZR0bLkquRDNUXg96tEUKje4'
    bot_chatID = '1259144189'
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id='+ bot_chatID + '&parse_mode=MarkdownV2&text=' + message

    response = requests.get(send_text)
    print(response.json())
    return response.json()

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

def index_json(csv_file):
    with open(csv_file, 'r') as file:
        csv_data = csv.reader(file)
        headers = next(csv_data)  # Get the header row
        json_data = {}

        for row in csv_data:
            node = row[1]  # First column as the node
            children = {header: value for header, value in zip(headers[2:], row[2:])}  # Rest of the columns as children

            if node not in json_data:
                json_data[node] = []

            json_data[node].append(children)

    return json_data

def down_non_zip(url):
    try:
        output_file = 'Test/' + url.split('/')[-1]

        response = requests.get(url, verify=False, timeout=3)
        if response.status_code == 200:
            with open(output_file, "wb") as file:
                file.write(response.content)
            print("CSV file downloaded successfully.")
            return 1
        else:
            print("Failed to download the CSV file.")
            return 0
    except:
       return 0

def down(URL):
  try:
    r = requests.get(URL, verify=False, timeout=3)
    print(r)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall("Test/")
    return 1
  except:
    return 0
try:
  os.makedirs("Test")
  dt = datetime.now()
  year = [str(dt.strftime("%m")).lower()]
  month = [str(dt.strftime("%Y")).upper()]
  stopDt = str(int(dt.strftime("%d")) + 1) +year[0] + month[0]
  print(stopDt)
  print('downloading......')
  for p in month:
      for j in year:
          for i in range(1,31):
              try:
                  year = p
                  dt = str(i) + j + year
                #   print(dt)
                  if str(dt) == str(stopDt):
                      break
                  if i < 10:
                      str1 = '0' + dt
                  else:
                      str1 = dt
                  print(str1)
                  URL = 'https://archives.nseindia.com/content/indices/ind_close_all_' + str(str1) + '.csv'
                #   print(URL)
                  resp = down_non_zip(URL)
                  if resp == 0:
                    pass
              except:
                print("passed")
                pass

  print('processing.....')
  path = "Test"
  dir_list = sorted(os.listdir(path))
  dir, dir1, dir2 = [], [], []
  for i in range(len(dir_list)):
    dir.append(dir_list[i].split('ind_close_all_')[1].split('.csv')[0])
    datetime_object = datetime.strptime(dir[i], '%d%m%Y')
    dir1.append(datetime_object.date())

  dir1 = sorted(dir1)
  print(dir1)
  for i in range(len(dir1)):
    d = dir1[i].strftime("%d%m%Y")
    fin = 'ind_close_all_'+d.upper()+'.csv'
    dir2.append(fin)
  dir_list = dir2
  dir_list.reverse()
  ticker = pd.read_csv("index_ticker.csv")
  ticker = ticker[["Index Name"]]
  df2 = ticker.merge(ticker, on='Index Name', how='left')
  for i in dir_list:
    path = "Test/"+str(i)
    df = pd.read_csv(path)
    df[i] = df["Change(%)"]
    df = df[["Index Name", i]]
    df2 = df2.merge(df, on='Index Name', how='left')

  #save file
  today = date.today()
  d4 = today.strftime('%b%Y')
  filename = 'CSVOutput/IndexChange_' + str(d4) + '.csv'
  df2.to_csv(filename)
  print('Process completed... file generated ', filename)

  json_data = index_json(filename)
  json_filename = 'JSONOutput/IndexChange_'+str(d4) + '.json'
  # dir_list_full = ['Test/' + i for i in dir_list]
  # json_data = multi_node_export(dir_list_full)
  save_file = open(json_filename, "w")  
  json.dump(json_data, save_file, indent = 4)
  save_file.close() 
 
  print(f"JSON file exported... {json_filename}")

  print(dir_list)
  dir_list_del = ['Test/'+ i for i in dir_list]
  for f in dir_list_del:
      os.remove(f)
  print("source files deleted...")
  os.rmdir("Test")
except Exception as e:
  print(e)
  msg("Process Stopped Need your attention")
   

# #add market cap at column
# #add current price column
# #52weeks high and low col
# #type of industry



