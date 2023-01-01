import os
import requests
from datetime import datetime


def get_puuid(platform, api, summonerNames, output_file):

    puuid_list = []
    for name in summonerNames:

        url = 'https://' + platform.lower() + \
            '.api.riotgames.com/lol/summoner/v4/summoners/by-name/' + name + '?api_key=' + api
        response = requests.get(url).json()
        if response.get('status'):
            print(response.get('status'))
            continue
        puuid = response['puuid']
        file.write(f"{puuid}\n")
        puuid_list.append(puuid)
        register_name_read(name)

        print(len(puuid_list))
    return puuid_list


def register_name_read(name):

    with open("./names/read_names.txt", "r") as f:
        read_lines = [line.rstrip() for line in f.readlines()]
    with open("./names/read_names.txt", "w") as f:
        f.writelines([line + "\n" for line in read_lines])
        f.write(f"{name}\n")
    f.close()


read_lines = []

if os.path.isfile("./names/read_name.txt"):
    with open("./names/read_name.txt", "r") as f:
        read_lines = [line.rstrip() for line in f.readlines()]
    f.close()

files_names = os.listdir("./names")
files_names = ["./names/"+name for name in files_names]
names = []
for file_name in files_names:
    with open(file_name, "r") as file:
        names.extend([line.rstrip()
                      for line in file.readlines() if line not in read_lines])
    file.close()

platform = "br1"
api_key = "api key"
file_name = f"./puuid/puuid-{datetime.now()}.txt"
print(f"names length ===>>>{len(names)}")
with open(file_name, "w") as file:
    puuid = get_puuid(platform, api_key, names, file)
file.close()
