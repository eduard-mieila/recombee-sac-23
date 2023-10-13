import csv
from recombee_api_client.api_client import RecombeeClient, Region
from recombee_api_client.api_requests import *

client = RecombeeClient('sac-upb-dev', 'x6p1TFmPseNHjO26HWoq1td8v3VYeAes1Un5dlq7luZunoLQOzfcCYEVvyviiC9B', region=Region.EU_WEST)

inFile = 'netflix_titles.csv'

# 2. Alegere și prelucrare dataset
# Dataset: https://www.kaggle.com/datasets/shivamb/netflix-shows/
with open(inFile, 'r') as file:
    reader = csv.reader(file, delimiter=",")
    fields = next(reader) # Read the header row
    entries = {}

    for row in reader:
        currId = 0
        currShow = {}

        for colIndex, propValue in enumerate(row):
            if (colIndex == 0):
                currId = propValue
            currShow[fields[colIndex]] = propValue
        
        entries[currId] = currShow
    print("Read all the data from '" + inFile + "'" )
    
    # 3. Implementare cod upload id-uri de produse 
    for itemId, _ in entries.items():
        client.send(AddItem(itemId))
    print("Sent all ids to Recombee DB")

    # 4. Implementare cod salvare proprietăți produse
    for propName in fields:
        client.send(AddItemProperty(propName, 'string'))
    print("Added all proprieties names to Recombee DB")
    
    #  5. Implementare cod salvare valori proprietăți produse
    for itemId, value in entries.items():
        client.send(SetItemValues(itemId, value, cascade_create = True))
    print("Added all proprieties values to Recombee DB")
    


