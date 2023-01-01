from datetime import datetime
import requests


def get_leagues(platform, api):
    """
    returns 200 summonerNames from master, grandmaster and challenger leagues in the given platform
    """

    leagues = ['challengerleagues', 'grandmasterleagues', 'masterleagues']

    url_challenger = 'https://' + platform + '.api.riotgames.com/lol/league/v4/' + \
        leagues[0] + '/by-queue/RANKED_SOLO_5x5?api_key=' + api
    response = requests.get(url_challenger)
    lis = response.json()['entries']
    summonerName_c = [sub['summonerName'] for sub in lis]

    url_grandmaster = 'https://' + platform + '.api.riotgames.com/lol/league/v4/' + \
        leagues[1] + '/by-queue/RANKED_SOLO_5x5?api_key=' + api
    response = requests.get(url_grandmaster)
    lis = response.json()['entries']
    summonerName_g = [sub['summonerName'] for sub in lis]

    url_master = 'https://' + platform + '.api.riotgames.com/lol/league/v4/' + \
        leagues[2] + '/by-queue/RANKED_SOLO_5x5?api_key=' + api
    response = requests.get(url_master)
    lis = response.json()['entries']
    summonerName_m = [sub['summonerName'] for sub in lis]

    summonerName = summonerName_c + summonerName_g + summonerName_m

    return summonerName


file_name = f"./names/names-{datetime.now()}.txt"
platform = "br1"
api_key = "api key"
names = get_leagues(platform, api_key)
with open(file_name, "w") as f:
    f.writelines([name + "\n" for name in names])
f.close()
