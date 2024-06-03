import os
import json
from elasticsearch import Elasticsearch
from tqdm import tqdm

client = Elasticsearch(hosts="http://101.43.216.60:9200/", api_key="OHhJbWJZOEJEN0cxSWJ6Z2NBQnc6eGRacF9GSWhTMUt4Z2hFcF9IM2ZmZw")

def read_json(file_name: str):
    with open(file_name, 'r') as f:
        data = json.load(f)
    return data

def add_directory(dir: str):
    for file_name in os.listdir(dir):
        if not file_name.endswith('.json'):
            continue
        data = read_json(f'{dir}/{file_name}')
        # 查询是否在es中
        if client.exists(index='music_demo', id=file_name.split('.')[0]):
            continue
        print(f'Adding {file_name} to elasticsearch')
        client.index(index='music_demo', document=data, id=file_name.split('.')[0])

platforms = ['damai', 'piaoxingqiu', 'gewara', 'fenwandao']
base_dir = 'data/unified_docs/'
for platform in platforms:
    add_directory(base_dir+platform)
