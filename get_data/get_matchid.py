import requests
import time
from datetime import datetime
import os
import json


def get_matchId(region, api, puuids, file):

    matchId_list = []
    for puuid in puuids:
        time.sleep(0.01)
        url = 'https://' + region + '.api.riotgames.com/lol/match/v5/matches/by-puuid/' + \
            puuid + '/ids?start=0&count=100&api_key=' + api
        response = requests.get(url).json()

        if isinstance(response, dict):
            print(response['status']['message'])
            time.sleep(40)
            url = 'https://' + region + '.api.riotgames.com/lol/match/v5/matches/by-puuid/' + \
                puuid + '/ids?start=0&count=10&api_key=' + api
            response = requests.get(url).json()
            if isinstance(response, dict):
                print(response.get('status'))
                continue

        matchId_list.extend(response)
        register_puuid_read(puuid)
        print(len(matchId_list))
        file.writelines([m+"\n" for m in matchId_list])

    return matchId_list


def register_puuid_read(puuid):
    read_lines = []
    if os.path.isfile("./puuid/read_puuid.txt"):
        with open("./puuid/read_puuid.txt", "r") as f:
            read_lines = [line.rstrip() for line in f.readlines()]
    with open("./puuid/read_puuid.txt", "w") as f:
        f.writelines([line + "\n" for line in read_lines])
        f.write(f"{puuid}\n")
    f.close()


read_lines = []

if os.path.isfile("./puuid/read_puuid.txt"):
    with open("./puuid/read_puuid.txt", "r") as f:
        read_lines = [line.rstrip() for line in f.readlines()]
    f.close()

files_names = os.listdir("./puuid")
files_names = ["./puuid/"+name for name in files_names]
puuids = []
for file_name in files_names:
    with open(file_name, "r") as file:
        puuids.extend([line.rstrip()
                      for line in file.readlines() if line not in read_lines])
    file.close()

puuids = list(dict.fromkeys(puuids))
print(f"puuids length ===>>>{len(puuids)}")

region = 'americas'
api_key = "api key"
file_name = f"./match/match-{datetime.now()}.txt"
with open(file_name, "w") as file:
    puuid = get_matchId(region, api_key, puuids, file)
file.close()
