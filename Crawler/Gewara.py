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

class GewaraCrawler(MusicCrawler):
    
    base_url = 'https://m.dianping.com/myshow/ajax/performances/%s;st=0;p=%s;s=10;tft=0?cityId=%s&sellChannel=7'
    detail_url = 'http://www.gewara.com/detail/'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Cookie': 'logan_session_token=8wa33wlkixrnw6fup8s6; logan_custom_report=; pvhistory="6L+U5ZuePjo8L3N1Z2dlc3QvZ2V0SnNvbkRhdGE/ZGV2aWNlX3N5c3RlbT1NQUNJTlRPU0gmeW9kYVJlYWR5PWg1JmNzZWNwbGF0Zm9ybT00JmNzZWN2ZXJzaW9uPTIuNC4wPjo8MTcxNTUxOTU0MDkwMV1fWw=="; m_flash2=1; _lxsdk_cuid=18f6ceece87c8-0cc2207c3e923e-1b525637-1fa400-18f6ceece87c8; _lxsdk=18f6ceece87c8-0cc2207c3e923e-1b525637-1fa400-18f6ceece87c8; _hc.v=e387f986-75d1-7b6f-c723-77863aa84234.1715519541; WEBDFPID=uuvy22679u9059wwy814x98v7uuw61u581u3xvvxu0y97958wu1uz800-2030879541033-1715519539854OMAAKCC75613c134b6a252faa6802015be905511940; GSI=#rf_syjgwlb@220829a#sl_zdjlwxbdsb@211011l#rf_syztwlb@210720a#rf_tsmbyrk@220309b',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9,zh;q=0.8,zh-CN;q=0.7',
        'Sec-Ch-Ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': "macOS",
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
    }
    platform = 'gewara'

    def __init__(self):
        super().__init__(self.platform)
        
    def crawl_origin_docs(self):
        self.exist_ids_list = self.get_exist_ids()
        self.origin_id_list = self.get_origin_ids()
        category_list = [1, 10]
        city_list = [1, 10, 20, 30, 40, 42, 44, 45, 50, 51, 52, 55, 56, 57, 59, 62, 66, 70, 73, 80]

        update_id_list = []
        details_df = pd.read_csv(f'data/meta_info/{self.platform}.csv')
        df_ids = list(details_df['performanceId'])

        for category in category_list:
            for city in city_list:
                print('正在爬取 category: %s, city: %s' % (str(category), str(city)))
                url = self.base_url % (str(category),'1',str(city))
                r = requests.get(url, headers=self.headers)
                json_r = json.loads(r.text)

                for detail_data in json_r['data']:
                    id = detail_data['performanceId']
                    if str(id) in self.exist_ids_list:
                        continue
                    if str(id) in self.origin_id_list:
                        continue
                    update_id_list.append(id)
                    if id not in df_ids:
                        new_detail_row = {'url': self.detail_url + str(detail_data['performanceId'])}
                        for col_ in details_df.columns[1:]:
                            if col_ == 'performanceLabelVO':
                                new_detail_row[col_] = str(detail_data[col_])
                            else:
                                new_detail_row[col_] = detail_data[col_]
                        details_df = pd.concat([details_df, pd.DataFrame([new_detail_row])], ignore_index=True)

                while json_r['paging']['hasMore'] == True:
                    pageNo = json_r['paging']['pageNo'] + 1
                    url = self.base_url % (str(category),str(pageNo),str(city))
                    r = requests.get(url, headers=self.headers)
                    json_r = json.loads(r.text)

                    for detail_data in json_r['data']:
                        id = detail_data['performanceId']
                        if str(id) in self.exist_ids_list:
                            continue
                        if str(id) in self.origin_id_list:
                            continue
                        update_id_list.append(id)
                        if id not in df_ids:
                            new_detail_row = {'url': self.detail_url + str(detail_data['performanceId'])}
                            for col_ in details_df.columns[1:]:
                                if col_ == 'performanceLabelVO':
                                    new_detail_row[col_] = str(detail_data[col_])
                                else:
                                    new_detail_row[col_] = detail_data[col_]
                            details_df = pd.concat([details_df, pd.DataFrame([new_detail_row])], ignore_index=True)
                    sleep(random.randint(1, 3))
                sleep(random.randint(1, 3))

        print(f'Update {len(update_id_list)} new items')
        print("update_id_list", update_id_list)
        details_df.to_csv(f'data/meta_info/{self.platform}.csv', index=False)
        for id in update_id_list:
        # tmp_update_id_list = [320924, 327013, 328782, 326869, 325904, 328468, 327368, 326678, 325876, 325834, 326506, 328366, 328477, 328437, 328378, 328361, 327996, 326639, 258819, 255201, 238341, 238336, 326089, 325926, 326666, 328236, 327631, 326704, 326189, 327763, 327701, 325963, 328866, 326019, 326503, 327887, 326623, 327859, 329058, 324371, 327049, 328669, 328071, 325898, 327745, 326652, 328926, 325170, 328926, 325170, 328356, 327761, 315421, 326656, 326008, 327343, 326160, 329058, 326624, 328389, 327467, 325278, 228843, 326946, 327067, 326711, 326405, 328926, 325170, 328449, 249766, 325928, 329296, 329296, 328449, 323275, 328173, 327379, 328098, 326134, 329296, 326535, 328039, 329055, 327951, 327979, 326134, 325928, 326693, 326693, 323275, 326693, 326693, 326535, 328039, 326134, 328666, 328173, 326693, 325928]
        # for id in tmp_update_id_list:
            # if id in origin_ids_list:
            #     continue
            json_docs = {}
            for col_ in details_df.columns:
                json_docs[col_] = str(details_df[details_df['performanceId']==id][col_].values[0])
            print(f'正在爬取 {id} details')
            detail_json = self.crawl_detail(id)

            json_docs.update(detail_json)
            if f'{id}.json' not in os.listdir(f'data/origin_docs/{self.platform}/'):
                with open(f'data/origin_docs/{self.platform}/{id}.json', 'w') as f:
                    json.dump(json_docs, f, indent=4, ensure_ascii=False)
            sleep(random.randint(1, 3))


    def crawl_detail(self, id):
        my_headers = [
            "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36",
            "Mozilla/5.0 (Linux; U; Android 2.3.6; en-us; Nexus S Build/GRK39F) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
            "Avant Browser/1.2.789rel1 (http://www.avantbrowser.com)",
            "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/532.5 (KHTML, like Gecko) Chrome/4.0.249.0 Safari/532.5",
            "Mozilla/5.0 (Windows; U; Windows NT 5.2; en-US) AppleWebKit/532.9 (KHTML, like Gecko) Chrome/5.0.310.0 Safari/532.9",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/534.7 (KHTML, like Gecko) Chrome/7.0.514.0 Safari/534.7",
            "Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US) AppleWebKit/534.14 (KHTML, like Gecko) Chrome/9.0.601.0 Safari/534.14",
            "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.14 (KHTML, like Gecko) Chrome/10.0.601.0 Safari/534.14",
            "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.20 (KHTML, like Gecko) Chrome/11.0.672.2 Safari/534.20",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.27 (KHTML, like Gecko) Chrome/12.0.712.0 Safari/534.27",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/13.0.782.24 Safari/535.1",
            "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/535.2 (KHTML, like Gecko) Chrome/15.0.874.120 Safari/535.2",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.7 (KHTML, like Gecko) Chrome/16.0.912.36 Safari/535.7",
            "Mozilla/5.0 (Windows; U; Windows NT 6.0 x64; en-US; rv:1.9pre) Gecko/2008072421 Minefield/3.0.2pre",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.10) Gecko/2009042316 Firefox/3.0.10",
            "Mozilla/5.0 (Windows; U; Windows NT 6.0; en-GB; rv:1.9.0.11) Gecko/2009060215 Firefox/3.0.11 (.NET CLR 3.5.30729)",
            "Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6 GTB5",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; tr; rv:1.9.2.8) Gecko/20100722 Firefox/3.6.8 ( .NET CLR 3.5.30729; .NET4.0E)",
            "Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
            "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
            "Mozilla/5.0 (Windows NT 5.1; rv:5.0) Gecko/20100101 Firefox/5.0",
            "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:6.0a2) Gecko/20110622 Firefox/6.0a2",
        ]

        detail_id_url = self.detail_url + str(id)
        self.headers['User-Agent'] = random.choice(my_headers)
        r = requests.get(detail_id_url, headers=self.headers)

        soup = BeautifulSoup(r.text, 'html.parser')
        # content_soup = soup.find_all('section', id='mainContent')
        content_soup = soup.find_all('div', attrs={'class': 'detailContent'})
        
        fail_cnt = 0
        if len(content_soup) == 0:
            for h in my_headers:
                self.headers['User-Agent'] = h
                r = requests.get(detail_id_url, headers=self.headers)
                soup = BeautifulSoup(r.text, 'html.parser')
                content_soup = soup.find_all('div', attrs={'class': 'detailContent'}) #detailContent
                if len(content_soup) == 0:
                    fail_cnt += 1
            if fail_cnt > 20:
                print(f'no data in {id}, fail_cnt: {fail_cnt}')
                assert False

        content_soup = content_soup[0]
        img_soup = content_soup.find_all('img')
        img_list = [img.get('src') for img in img_soup]

        p_soup = content_soup.find_all('p')
        text_list = [p.text for p in p_soup if p.text != '']

        json_details = { 
            'img_list': img_list,
            'text_list': text_list
        }
        return json_details
    
    def unify_docs(self):
        origin_ids_list = self.get_origin_ids()
        exist_ids_list = self.get_exist_ids()
        for id in origin_ids_list:
            if id not in exist_ids_list:
                self.unify_one_docs(id)
    
    def unify_one_docs(self, origin_id):
        ticketStatus_dict = {
            '0': '即将预售',
            '1': '即将开售',
            '2': '预售',
            '3': '售票中', 
            '12': '演出延期中',
        }
        path = f'data/origin_docs/{self.platform}/{origin_id}.json'
        
        with open(path, 'r') as f:
            file_json = json.load(f)
            unified_json_docs = {
                "platform": "猫眼",
                "project_id": file_json['performanceId'],
                "project_name": file_json['name'],
                "category_name": file_json['cornerDisplayName'],
                "artists": [],  # 无信息
                "show_status": ticketStatus_dict[file_json['ticketStatus']],       #["PRESALE"?, 售票中] TODO 查看details_df的unique
                
                "show_time": file_json['showTimeRange'], #"2024.06.15 周六 20:00"
                "session_time": [],
                "price": file_json['priceRange'],  # 下面再根据maxOriginalPriceInfo改

                "isGeneralAgent": "true" if file_json['generalAgent']=="1" else "false",  # 总票代
                "isHotProject": "false",
                "rating": None,

                "city_name": file_json['cityName'],
                "venue_name": file_json['shopName'],
                "venue_info": {
                    "venue_address": file_json['address'],
                    "lng": file_json['lng'],
                    "lat": file_json['lat'],
                },

                "project_info": "\n".join(file_json['text_list']),         # 提取damai "itemExtendInfo" "itemExtend" HTML中的文本
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
                "project_link": "https://www.gewara.com/detail/" + file_json['performanceId'],
            }
            unified_json_docs["project_imgs"].extend(file_json['img_list'][1:])
            if "/" in unified_json_docs['show_time']:
                unified_json_docs['session_time'] = unified_json_docs['show_time'].split('/')

            with open(f'data/unified_docs/{self.platform}/{origin_id}.json', 'w') as f:
                json.dump(unified_json_docs, f, ensure_ascii=False, indent=4)