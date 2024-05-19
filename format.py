import os
import json
import requests
import urllib.parse
from tqdm import tqdm
import pandas as pd


city_df = pd.read_csv('data/city_code.csv', sep='\t')

def read_json(file_name: str):
    with open(file_name, 'r') as f:
        data = json.load(f)
    return data

def get_city(city_name: str, venue_name: str, venue_addr: str | None):
    if city_name == 'nan':
        return 'nan'
    if city_name == '中国澳门' or '中国香港':
        return city_name
    print(city_name)
    for i in range(len(city_df)):
        if city_df.loc[i, 'cityName'].startswith(city_name):
            return city_df.loc[i, 'cityName'].strip()
    
    if venue_name == 'nan' and venue_addr is None:
        return city_name
    param = venue_addr + ' ' + venue_name
    encode_param = urllib.parse.quote(param)
    response = requests.get(f'https://restapi.amap.com/v3/geocode/geo?address={encode_param}&key=8b9b85badd591da30ce03552e64bcf5a')
    # print(response.json())
    return response.json()['geocodes'][0]['city']

def process_city(dir: str):
    for file_name in tqdm(os.listdir(dir)):
        if not file_name.endswith('.json'):
            continue
        data = read_json(f'{dir}/{file_name}')
        # print(file_name)
        data['city_name'] = get_city(data['city_name'], data['venue_name'], data['venue_info']['venue_address'])
        with open(f'{dir}/{file_name}', 'w') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

def get_artists(platforms, global_artists):
    for platform in platforms:
        exist_files = os.listdir(f'data/unified_docs/{platform}')
        exist_ids = [file.split('.')[0] for file in exist_files]
        for exist_id in exist_ids:
            with open(f'data/unified_docs/{platform}/{exist_id}.json', 'r') as f:
                file_json = json.load(f)
                file_artists = file_json['artists']

                if len(file_artists) == 1 and file_artists != '以现场为准':
                    if '/ ' in file_artists[0]:
                        file_json['artists'] = file_artists[0].split('/ ')
                        # print(f'{platform} {exist_id}')
                        for artist in file_json['artists']:
                            if artist not in global_artists:
                                global_artists.append(artist)
                    elif '/' in file_artists[0]:
                        file_json['artists'] = file_artists[0].split('/')
                        # print(f'{platform} {exist_id}')
                        for artist in file_json['artists']:
                            if artist not in global_artists:
                                global_artists.append(artist)
                    elif '，' in file_artists[0]:
                        file_json['artists'] = file_artists[0].split('，')
                        # print(f'{platform} {exist_id}')
                        for artist in file_json['artists']:
                            if artist not in global_artists:
                                global_artists.append(artist)
                    elif '、' in file_artists[0]:
                        file_json['artists'] = file_artists[0].split('、')
                        # print(f'{platform} {exist_id}')
                        for artist in file_json['artists']:
                            if artist not in global_artists:
                                global_artists.append(artist)
                    elif '；' in file_artists[0]:
                        file_json['artists'] = file_artists[0].split('；')
                        print(f'{platform} {exist_id}')
                        for artist in file_json['artists']:
                            if artist not in global_artists:
                                global_artists.append(artist)
                with open(f'data/unified_docs/{platform}/{exist_id}.json', 'w') as f:
                    json.dump(file_json, f, ensure_ascii=False, indent=4)
    return global_artists

def process_artists(dir: str, global_artists):
    for file_name in tqdm(os.listdir(dir)):
        if not file_name.endswith('.json'):
            continue
        file_json = read_json(f'{dir}/{file_name}')
        file_artists = file_json['artists']

        if (len(file_artists) == 1 and file_artists == '以现场为准') or (len(file_artists) == 0):
            
            file_json['artists'] = []
            for artist in global_artists:
                if artist in file_json['project_name']:
                    file_json['artists'].append(artist)
            with open(f'{dir}/{file_name}', 'w') as f:
                json.dump(file_json, f, ensure_ascii=False, indent=4)

platforms = ['damai', 'piaoxingqiu', 'gewara', 'fenwandao']
base_dir = 'data/unified_docs/'
with open('data/global_artists.json', 'r') as f:
    file_json = json.load(f)
    global_artists = file_json['global_artists']
global_artists = get_artists(platforms, global_artists)
with open('data/global_artists.json', 'w') as f:
    json.dump({'global_artists': global_artists}, f, ensure_ascii=False, indent=4)

for platform in platforms:
    process_city(base_dir+platform)
    process_artists(base_dir+platform, global_artists)