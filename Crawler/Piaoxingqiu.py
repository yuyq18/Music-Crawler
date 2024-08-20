import os
import hashlib
import requests
import time
from time import sleep
import random
import json
import pandas as pd
from bs4 import BeautifulSoup
from Crawler.Base import MusicCrawler

class PiaoxingqiuCrawler(MusicCrawler):
    base_url = 'https://m.piaoxingqiu.com/cyy_gatewayapi/home/pub/v3/show_list/search_by_front'
    
    detail_url = 'https://m.piaoxingqiu.com/cyy_gatewayapi/show/pub/v5/show/%s/static'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Cookie': 'sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%2218f23ddc1ed5bc-01413f23bfaa98b-1b525637-1484784-18f23ddc1ee8b3%22%2C%22first_id%22%3A%22%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMThmMjNkZGMxZWQ1YmMtMDE0MTNmMjNiZmFhOThiLTFiNTI1NjM3LTE0ODQ3ODQtMThmMjNkZGMxZWU4YjMifQ%3D%3D%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%22%2C%22value%22%3A%22%22%7D%2C%22%24device_id%22%3A%2218f23ddc1ed5bc-01413f23bfaa98b-1b525637-1484784-18f23ddc1ee8b3%22%7D; acw_tc=276077c817155819186183972e14966ee34cf256340918d3d2e253c37eecc2; ssxmod_itna=eqRhDIqjOG8DCDyDl4iu7t+t7xQTuR2eqrKDstTDSxGKidDqxBnnkcuYGQ5Shbv4goTwxStrB22YSKCSxmTz+C8eKD=xYQDwxYoDUxGtDpxG6QwDenQD5xGoDPxDeDAmKDCydNKDdRHV9CH=F7OaICDYc7sxDOwxGCi4GtVpyzR1ptRxDWN40k/xiasx0Cy=9FDmKDIpS7Q35DFxoEESgHDmRojgEvDC9pG6PDUW5zS=KUTB2xb=RtNQrqEB2DNiiP5Bh4amhce0Wt7vh5NmTxNWh4OQha6XgD5GryqYD===; ssxmod_itna2=eqRhDIqjOG8DCDyDl4iu7t+t7xQTuR2eqxikIz3NDlgZCDj4GQ=a2=eqNzjqzn+cbrjx2+BuY0x57rYYRY2Db0arhj8C6cbjuUFcAjOuf0SIjFbTu8=9527uQzjfN=985WsNxI5/HhZn=hoprYE2W7=0=K2mN8oqbrFvNRYypa45kk+PrjrmrargikpP1aZgr6h3Ljxido/8O8=0DQ0RLzn7QIZ8PlFcL=lcH6Qjf5LiI5vTLqM92uDejaiE0o4L5sxMT0f39WwcL13Q87owwaQqdRDe+3FpMAyM+xc+Q8pHZRwqZWpdpQU+=0Mpd4zjU7tq/LLGe1xIq83Y33o8jCotEPehZPlYrNKLYKbiSAapojr5lOfT9UFptTSRW9b9lbaSrI=u3auC8TSow/ptP74DQFqRh4QuCI2qfcD/+roDbT4=78rC8r8D+bv5Qm60h5n2WVrH1nnD08DijqYD; tfstk=fEkr__g_fJ0XC8c3urwUuytNXpw8IRL1YvaQxDm3Vz4oVDmHL2gAR0iIFJvEfrM7P0wl2j3sjTMWwJ6UeJeH5F96CQn8pJm7rqURy-q3XpZuZ7cf7Pje5F96hTXr3STsOQ4M0SrLouqlEJxVnuU0qaqo-rV0jl_uKJ0nmnrTYyquq_vcokqo_xF_TFrQZUekeS9OI_P7S04rV0McMS5-VrlmgxqrBdn8uSNj3uPzSSwdemDZj0DTYvKGTyotZ4Zx8d84nYurESyeLteSbj0zgb8F3ShmXvVqMF5tX00rs5Dk3nFqecwuuvKdpzcSxvNEEFjQrY3S35HdzEaI1mMugVYf32FEZf2oLFJc41bLmBDxpb7hT7qY0Pt2mFPDZ3RXw7If96FRzoz6qXfd97V70Pt206CLwKZ453ch.',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Referer': 'https://m.piaoxingqiu.com/',
        'Accept-Language': 'en-US,en;q=0.9,zh;q=0.8,zh-CN;q=0.7',
        'Sec-Ch-Ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': "macOS",
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
    }
    cookie_template = 'cna=Kvm0HlkLLxMCAbetkdHo8vj+; xlly_s=1; mtop_partitioned_detect=1; _m_h5_tk=; _m_h5_tk_enc=; tfstk=fVvsTAOaMEpeQatCo1nFdlkwiPBj4FMrGosvqneaDOB9hBTkl1oM_nQXGwKDuNyN7eMXqU1NMK72hm6MLCyV7VXXDUWjz4krUhxGCt3rzoVpPdBADx7vOvNkVtXxz2rGSQ15nUJDLkhXvDsV0RFvkZKdJwjODtQOMyFd2wQAHtLAp6IfqPIA6GBLJUussMrf-hgqA4e0gkS2XwwJYpsOs5xOR-y2dC51yx_QH-pCfHYxsm2tFwdwMdWpB2UVp3AeWM6ShosBNHBOfEDuIOKJvIC6hjrloBtpa6O3bcjBC39fV6cYuZ5WpdWMO4wPyBxvh6KiP7jeM3JwGFu3HNRWve1yLyyPpdT6B66R4_ePPsK_Gk1uhM_rADN0gdk-Aq3qH92c6MjUDDiQaRfOxM6mADN0i1IhYbmIA7yG.; isg=BFFRjvzi0yQ8Xz9aqLz2wdYwYFvrvsUwPYnpwjPmT5g32nEsew-tAPR7fa48VV1o'
    platform = 'piaoxingqiu'
    def __init__(self):
        super().__init__(self.platform)
        
    
    def crawl_origin_docs(self):
        self.exist_ids_list = self.get_exist_ids()
        self.origin_id_list = self.get_origin_ids()
        params = {
            'bizFrontendCategoryId': '63f9bed409eccc0001cc32aa',
            'cityId': '1101',
            'length': '10',
            'offset': '0',
            'pageIndex': '0',
            'pageLength': '10',
            'pageType': 'ALL_PAGE',
            'src': 'WEB',
            'ver': '4.5.1',
        }
        category_list = ['63f9bed409eccc0001cc32aa', '64813d19205bc10001978734']

        update_id_list = []
        details_df = pd.read_csv(f'data/meta_info/{self.platform}.csv')
        df_ids = list(details_df['showId'])

        for category in category_list:
            params['bizFrontendCategoryId'] = category
            params['offset'] = str(0)
            r = requests.get(self.base_url, headers=self.headers, params=params)
            json_r = json.loads(r.text)

            for detail_data in json_r['data']['searchData']:
                id = detail_data['showId']
                if str(id) in self.exist_ids_list:
                        continue
                if str(id) in self.origin_id_list:
                    continue
                update_id_list.append(id)
                if id not in df_ids:
                    new_detail_row = {}
                    for col_ in details_df.columns:
                        new_detail_row[col_] = detail_data[col_]
                    details_df = pd.concat([details_df, pd.DataFrame([new_detail_row])], ignore_index=True)

            while json_r['data']['isLastPage'] == False:
                params['offset'] = str(int(params['offset']) + int(params['length']))
                r = requests.get(self.base_url, headers=self.headers, params=params)
                json_r = json.loads(r.text)

                for detail_data in json_r['data']['searchData']:
                    id = detail_data['showId']
                    if str(id) in self.exist_ids_list:
                            continue
                    if str(id) in self.origin_id_list:
                        continue
                    update_id_list.append(id)
                    if id not in df_ids:
                        new_detail_row = {}
                        for col_ in details_df.columns:
                            new_detail_row[col_] = detail_data[col_]
                        details_df = pd.concat([details_df, pd.DataFrame([new_detail_row])], ignore_index=True)
                sleep(random.randint(1, 3))
            sleep(random.randint(1, 3))

        print(f'Update {len(update_id_list)} new items')
        print("update_id_list", update_id_list)

        details_df.to_csv(f'data/meta_info/{self.platform}.csv', index=False)

        for id in update_id_list:
            json_docs = {}
            print(f'正在爬取 {id} details')
            for col_ in details_df.columns:
                json_docs[col_] = str(details_df[details_df['showId']==id][col_].values[0])
            
            detail_json = self.crawl_detail(id)
            if detail_json:
                json_docs.update(detail_json)
                if f'{id}.json' not in os.listdir(f'data/origin_docs/{self.platform}/'):
                    with open(f'data/origin_docs/{self.platform}/{id}.json', 'w') as f:
                        json.dump(json_docs, f, indent=4, ensure_ascii=False)
                sleep(random.randint(1, 3))

    def crawl_detail(self, id):
        r = requests.get(self.detail_url % id, headers=self.headers)
        json_r = json.loads(r.text)
        if 'data' in json_r:
            return json_r['data']

    def unify_docs(self):
        origin_ids_list = self.get_origin_ids()
        exist_ids_list = self.get_exist_ids()
        for id in origin_ids_list:
            if id not in exist_ids_list:
                self.unify_one_docs(id)

    def unify_one_docs(self, origin_id):
        showStatus_dict = {
            'PRESALE': '预售',
            'PENDING': '待定',
            'ONSALE': '售票中', 
            'DELAY': '延期',
        }

        path = f'data/origin_docs/{self.platform}/{origin_id}.json'
        
        with open(path, 'r') as f:
            file_json = json.load(f)

            unified_json_docs = {
                "platform": "票星球",
                "project_id": file_json['showId'],
                "project_name": file_json['showName'],
                "category_name": "",
                "artists": [],  # 根据 observationInstructions 设置
                "show_status": showStatus_dict[file_json['showStatus']],       #["PRESALE"?, 售票中] TODO 查看details_df的unique
                
                "show_time": file_json['showDate'], #"2024.06.15 周六 20:00"
                "session_time": eval(file_json['showSessionDates']),
                "price": file_json['minOriginalPrice'],  # 下面再根据maxOriginalPriceInfo改

                "isGeneralAgent": "false",  # 总票代
                "isHotProject": "false",
                "rating": None,

                "city_name": file_json['cityName'],
                "venue_name": file_json['venueName'],
                "venue_info": {
                    "venue_address": None,
                    "lng": None,
                    "lat": None,
                },

                "project_info": "",         # 提取damai "itemExtendInfo" "itemExtend" HTML中的文本
                "project_imgs": [
                    file_json['posterUrl']
                ],
                "wantVO": {
                    "wantNum": None,
                    "wantNumStr": None,
                    "wantNumSuffix": None,
                    "wantDesc": None
                },
                "tours": [],
                "project_link": 'https://m.piaoxingqiu.com/content/'+str(file_json['showId'])+'?showId='+str(file_json['showId']),
            }
            category_dict = eval(file_json["backendCategory"])
            unified_json_docs["category_name"] = category_dict["displayName"]
            # 处理 artists 等
            if "basicInfo" in file_json:
                basicInfo_json =  file_json["basicInfo"]
                if 'maxOriginalPriceInfo' in basicInfo_json and 'yuanNum' in basicInfo_json['maxOriginalPriceInfo']:
                    unified_json_docs['price'] =  basicInfo_json['minOriginalPriceInfo']['yuanNum'] + '-' + basicInfo_json['maxOriginalPriceInfo']['yuanNum']
                ###########################################################################   
                if 'hotMode' in basicInfo_json:
                    unified_json_docs['isHotProject'] = "true" if basicInfo_json['hotMode'] else "false"
                ###########################################################################
                if 'venueAddress' in basicInfo_json:
                    unified_json_docs['venue_info']['venue_address'] = basicInfo_json['venueAddress']
                if 'venueLat' in basicInfo_json:
                    unified_json_docs['venue_info']['lat'] = basicInfo_json['venueLat']
                if 'venueLng' in basicInfo_json:
                    unified_json_docs['venue_info']['lng'] = basicInfo_json['venueLng']
                ########################################################################### 
                if 'contentUrl' in basicInfo_json:
                    content_r = requests.get(basicInfo_json['contentUrl'])
                    soup = BeautifulSoup(content_r.text, 'html.parser')
                    text_list = []
                    text_soup  = soup.find_all('p')
                    for p_ in text_soup:
                        text_list.append(p_.text)
                    unified_json_docs["project_info"] = "\n".join(text_list)

                    img_soup = soup.find_all('img')
                    for img_ in img_soup:
                        unified_json_docs["project_imgs"].append(img_.get('src'))
                ###########################################################################
                else:
                    print(f"basicInfo not in {origin_id}")
                    assert False
                if 'descInfo' in file_json and 'observationInstructions' in file_json['descInfo']:
                    for obs in file_json['descInfo']['observationInstructions']:
                        if obs['key'] == 'MAIN_ACTOR':
                            unified_json_docs['artists'].append(obs['value'])

                with open(f'data/unified_docs/{self.platform}/{origin_id}.json', 'w') as f:
                    json.dump(unified_json_docs, f, ensure_ascii=False, indent=4)


if __name__ == '__main__':
    piaoxingqiu = PiaoxingqiuCrawler()
    # piaoxingqiu.crawl_origin_docs()
    test_id = 344122
    piaoxingqiu.crawl_detail(test_id)
    piaoxingqiu.unify_one_docs(test_id)
