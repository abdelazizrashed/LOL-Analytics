from datetime import datetime
import json
import os
import random
import requests
import time


def get_matches(region, api, matchIds):

    matches = []
    i = 0
#   random_indices = random.sample(range(0, len(matchIds)-1), 1000)
    for ind in range(len(matchIds)):

        time.sleep(0.01)
        matchid = matchIds[ind].rstrip()
        url = 'https://' + region + '.api.riotgames.com/lol/match/v5/matches/' + \
            matchid + '?api_key=' + api
        response = requests.get(url).json()

        # If rate limit is exceeded
        while response.get('status'):
            print(response['status']['message'])
            time.sleep(40)
            url = 'https://' + region + '.api.riotgames.com/lol/match/v5/matches/' + \
                matchid + '?api_key=' + api
            response = requests.get(url).json()

        i += 1
        print(i)
        matches.append(response)
        register_matchid_read(matchid)
        write_match_json(response, matchid)

    return matches


def register_matchid_read(matchId):
    read_lines = []
    if os.path.isfile("./match/read_match.txt"):
        with open("./match/read_match.txt", "r") as f:
            read_lines = [line.rstrip() for line in f.readlines()]
    with open("./match/read_match.txt", "w") as f:
        f.writelines([line + "\n" for line in read_lines])
        f.write(f"{matchId}\n")
    f.close()


read_lines = []

if os.path.isfile("./match/read_match.txt"):
    with open("./match/read_match.txt", "r") as f:
        read_lines = [line.rstrip() for line in f.readlines()]
    f.close()
print(f"read_lines length ====>>>> {len(read_lines)}")

files_names = os.listdir("./match")
files_names = ["./match/"+name for name in files_names]
matchids = []
for file_name in files_names:
    with open(file_name, "r") as file:
        lines = file.readlines()
        print(f"lines length ====>>>> {len(lines)}")
        matchids.extend(lines)
    file.close()

print(f"matchids length before clean ===>>>{len(matchids)}")
matchids = list(dict.fromkeys(matchids))
print(f"matchids length ===>>>{len(matchids)}")
matchids = [line.rstrip()
            for line in matchids if line.rstrip() not in read_lines]
random.shuffle(matchids)
print(f"matchids length ===>>>{len(matchids)}")

region = 'americas'

api_key = "RGAPI-977f6fc6-eb89-4d35-883c-615aa4e81b5c"


def write_match_json(j: dict, id: str):
    path = f"./match_data/{id}.json"
    if os.path.isfile(path):
        return
    with open(path, 'w') as f:
        json.dump(j, f)
    f.close()


puuid = get_matches(region, api_key, matchids)
