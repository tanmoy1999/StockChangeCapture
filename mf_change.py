import pandas as pd
import requests
import csv
import json

def mffull():
    k = 'https://www.amfiindia.com/spages/NAVAll.txt'
    response = requests.get(k)
    df = pd.DataFrame(columns=['Scheme Code','ISIN Div Payout/ ISIN Growth','ISIN Div Reinvestment','Scheme Name','Net Asset Value','Date'])
    data = response.text.split("\n")
    for scheme_data in data:
        if ";INF" in scheme_data:
            scheme = scheme_data.split(";")
            scheme[5] = scheme[5].replace('\r','')
            if '2023' in scheme[5]:
                df.loc[len(df)] = scheme
                print('2023')
    return df

mffull().to_csv('mf.csv')


csv_file = 'mf.csv'
json_file = 'data.json'

data = {}

with open(csv_file, 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        scheme_name = row['Scheme Name']
        data = {
            'Scheme Name': row['Scheme Name'],
            'Scheme Code': int(row['Scheme Code']),
            'ISIN Div Payout/ ISIN Growth': row['ISIN Div Payout/ ISIN Growth'],
            'ISIN Div Reinvestment': row['ISIN Div Reinvestment'],
            'Net Asset Value': float(row['Net Asset Value']),
            'Date': row['Date']
        }

with open(json_file, 'w') as file:
    json.dump(data, file, indent=2)

print(f"CSV file '{csv_file}' converted to JSON file '{json_file}' successfully.")
