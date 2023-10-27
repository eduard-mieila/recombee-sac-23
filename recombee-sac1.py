import csv
from recombee_api_client.api_client import RecombeeClient, Region
from recombee_api_client.api_requests import *

def printRecommandations(user, rec):
    print('Recommandations for user ' + user)
    for r in rec["recomms"]:
        print("\t" + r["id"])
    print("\n")

client = RecombeeClient('sac-upb-dev', 'x6p1TFmPseNHjO26HWoq1td8v3VYeAes1Un5dlq7luZunoLQOzfcCYEVvyviiC9B', region=Region.EU_WEST)

inFileMovies = 'netflix_titles_medium.csv'
inFileUsers = 'users.csv'

# I.2. Alegere și prelucrare dataset
# Dataset: https://www.kaggle.com/datasets/shivamb/netflix-shows/
with open(inFileMovies, 'r') as file:
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
    print("Read all the data from '" + inFileMovies + "'" )
    
    # # I.3. Implementare cod upload id-uri de produse 
    requests = []
    for itemId, _ in entries.items():
        r = AddItem(itemId)
        # Delete pentru resetare DB
        # r = DeleteItem(itemId)
        requests.append(r)
    client.send(Batch(requests))
    print("Sent all ids to Recombee DB")

    # # I.4. Implementare cod salvare proprietăți produse
    requests = []
    for propName in fields:
        r = AddItemProperty(propName, 'string')
        requests.append(r)
    client.send(Batch(requests))
    print("Added all proprieties names to Recombee DB")
    
    # # I.5. Implementare cod salvare valori proprietăți produse
    for itemId, value in entries.items():
        r = SetItemValues(itemId, value, cascade_create = True)
        requests.append(r)
    client.send(Batch(requests))
    print("Added all proprieties values to Recombee DB")
    

with open(inFileUsers, 'r',  encoding='utf-8-sig') as file:
    reader = csv.reader(file, delimiter=",")
    userFields = next(reader) # Read the header row
    users = {}

    for row in reader:
        currId = 0
        currUser = {}

        for colIndex, propValue in enumerate(row):
            if (colIndex == 0):
                currId = propValue
            currUser[userFields[colIndex]] = propValue
        
        users[currId] = currUser
    print("Read all the data from '" + inFileUsers + "'" )
    
    # # II.1. Adauga id-uri client
    requests = []
    for userId, _ in users.items():
        r = AddUser(userId)
        # Delete pentru resetare DB
        # r = DeleteUser(userId)
        requests.append(r)
    client.send(Batch(requests))
    print("Sent all user ids to Recombee DB")

    # # II.2. Adauga interactiuni user-movie
    interactions = []
    interactions.append(AddDetailView("u001", "s1"))
    interactions.append(AddDetailView("u001", "s13"))
    interactions.append(AddDetailView("u001", "s32"))
    interactions.append(AddDetailView("u001", "s23"))

    interactions.append(AddDetailView("u002", "s73"))
    interactions.append(AddDetailView("u002", "s19"))

    interactions.append(AddDetailView("u003", "s44"))
    interactions.append(AddDetailView("u003", "s24"))
    interactions.append(AddDetailView("u003", "s32"))
    interactions.append(AddDetailView("u003", "s72"))

    interactions.append(AddDetailView("u004", "s92"))
    interactions.append(AddDetailView("u004", "s94"))
    interactions.append(AddDetailView("u004", "s21"))

    interactions.append(AddDetailView("u005", "s32"))
    interactions.append(AddDetailView("u005", "s2"))

    interactions.append(AddDetailView("u006", "s7"))
    interactions.append(AddDetailView("u006", "s6"))
    interactions.append(AddDetailView("u006", "s9"))
    
    interactions.append(AddDetailView("u007", "s36"))
    
    interactions.append(AddDetailView("u008", "s64"))
    
    interactions.append(AddDetailView("u009", "s88"))
    
    interactions.append(AddDetailView("u010", "s26"))

    client.send(Batch(interactions))


    # # II.3. Recomanda cate 5 id-uri de filme/seriale pentru fiecare utilizator
    for u in users:
        r = client.send(RecommendItemsToUser(u, 5))
        printRecommandations(u, r)
        
