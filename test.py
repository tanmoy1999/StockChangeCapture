import csv
import json

def csv_to_json(csv_file):
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

csv_file = r'C:\Users\tanmo\Desktop\Stock\BhavCopyAnalysis\May20231.csv'  # Replace with the path to your CSV file
json_data = csv_to_json(csv_file)
print(json_data)

save_file = open("sample.json", "w")  
json.dump(json_data, save_file, indent = 6)  
save_file.close()  

