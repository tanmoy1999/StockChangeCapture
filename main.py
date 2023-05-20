import os
import pandas as pd
import numpy as np
import wget
import zipfile
from datetime import datetime, date

import requests, zipfile, io

def down(URL):
  try:
    r = requests.get(URL, verify=False, timeout=3)
    print(r)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall("Test/")
    return 1
  except:
    return 0

os.makedirs("Test")
dt = datetime.now()
year = [str(dt.strftime("%b")).upper()]
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
                # print(dt)
                if str(dt) == str(stopDt):
                    break
                if i < 10:
                    str1 = 'cm0' + dt +'bhav.csv.zip'
                else:
                    str1 = 'cm' + dt +'bhav.csv.zip'
                print(str1)
                URL = 'https://archives.nseindia.com/content/historical/EQUITIES/'+year+'/' + j + '/' + str1
                # print(URL)
                resp = down(URL)
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
  dir.append(dir_list[i].split('cm')[1].split('bhav')[0])
  datetime_object = datetime.strptime(dir[i], '%d%b%Y')
  dir1.append(datetime_object.date())

dir1 = sorted(dir1)
for i in range(len(dir1)):
  d = dir1[i].strftime("%d%b%Y")
  fin = 'cm'+d.upper()+'bhav.csv'
  dir2.append(fin)
dir_list = dir2
dir_list.reverse()
ticker = pd.read_csv("ticker.csv")
ticker = ticker[["SYMBOL"]]
df2 = ticker.merge(ticker, on='SYMBOL', how='left')
for i in dir_list:
  path = "Test/"+str(i)
  prevClose = 'PREVCLOSE_'+str(i)
  close = 'CLOSE_'+str(i)
  df = pd.read_csv(path)
  df = df[df['SERIES'] == "EQ"]
  df.rename(columns = {'PREVCLOSE':prevClose, 'CLOSE':close}, inplace = True)
  df[i] = ((df[close] - df[prevClose])/df[prevClose]) * 100
  df = df[["SYMBOL", i]]
  df2 = df2.merge(df, on='SYMBOL', how='left')

#save file
today = date.today()
d4 = today.strftime('%b%Y')
filename = str(d4) + '.csv'
df2.to_csv(filename)
print('Process completed... file generated ', filename)
print(dir_list)
dir_list_del = ['Test/'+ i for i in dir_list]
for f in dir_list_del:
    os.remove(f)
print("source files deleted...")
os.rmdir("Test")

#add market cap at column
#add current price column
#52weeks high and low col
#type of industry
