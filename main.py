
import os
import hashlib
import requests
import time
from time import sleep
import random
import json
import pandas as pd
from bs4 import BeautifulSoup

class MusicCrawler:
    def __init__(self, platform):
        self.platform = platform
    
    # def read_df(self):
    #     path = f'data'
    #     details_df = pd.read_csv('details.csv')
    def get_exist_ids(self):
        path = f'data/unified_docs/{self.platform}/'
        exist_id_files = os.listdir(path)
        exist_ids = [file.split('.')[0] for file in exist_id_files]
        return exist_ids

    def get_origin_ids(self):
        path = f'data/origin_docs/{self.platform}/'
        origin_id_files = os.listdir(path)
        origin_ids = [file.split('.')[0] for file in origin_id_files]
        return origin_ids

class DamaiCrawler(MusicCrawler):
    base_url = 'https://search.damai.cn/searchajax.html'
    detail_url = 'https://mtop.damai.cn/h5/mtop.damai.item.detail.getdetail/1.0/'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36 Edge/16.16299',
        'Cookie': 'cna=Kvm0HlkLLxMCAbetkdHo8vj+; xlly_s=1; XSRF-TOKEN=65e91fd3-9574-4c58-a160-885d824a24e1; tfstk=fDJxtVmVf0EAXDhMhnGk7u2_zRno-KKVwE-QINb01ULJfenVjqJMwGLMym-DfqR9yhTK0G02IQE9YULO_m7G5NLMfImoKvx20OW6X2DnKJ429RvA5PX6ugE8Q20nKYcbVO3x-C4fvNWRbasfc-_j2asGjOwbhF_5NMs3lO6653iRfiFbc-_bVus6zp2P7f_3BLfLmkjs4Ze_CLHNMwKbsRw6eiCAtn_JmnvRDsQCZ67hLpKBcpS9QDNOWHRXog8tp4BWHEpfwOHTUtxpNECJMVeRaQteC_drJW-ecEdCOKM-4_p2xt_JQcyNlIx6I1pjRlbX3EvRTdzxrNAHqK1JR42J71dXOt9tJYszVpvdEK4hJGVj20Fa_sskzbI-0U4Et4IR-0Yb_558TgQn2jNa_ssP2wmlU5PNwW5..; isg=BMnJICw6y0MBObci0MT-6S7Y2PMjFr1IdbFhWms-RbDvsunEs2bNGLfg9RYEvFWA',
        'X-Xsrf-Token': '7d065ac9-b924-4c14-869a-ab599b571244',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9,zh;q=0.8,zh-CN;q=0.7',
        'Bx-V': '2.5.0',
        'Referer': 'https://search.damai.cn/search.htm?spm=a2oeg.home.category.ditem_0.591b23e1HxE6Vj&ctl=%E6%BC%94%E5%94%B1%E4%BC%9A&order=1&cty=%E6%88%90%E9%83%BD',
        'Sec-Ch-Ua': '"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': "macOS",
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
    }
    cookie_template = 'cna=Kvm0HlkLLxMCAbetkdHo8vj+; xlly_s=1; mtop_partitioned_detect=1; _m_h5_tk=; _m_h5_tk_enc=; tfstk=fVvsTAOaMEpeQatCo1nFdlkwiPBj4FMrGosvqneaDOB9hBTkl1oM_nQXGwKDuNyN7eMXqU1NMK72hm6MLCyV7VXXDUWjz4krUhxGCt3rzoVpPdBADx7vOvNkVtXxz2rGSQ15nUJDLkhXvDsV0RFvkZKdJwjODtQOMyFd2wQAHtLAp6IfqPIA6GBLJUussMrf-hgqA4e0gkS2XwwJYpsOs5xOR-y2dC51yx_QH-pCfHYxsm2tFwdwMdWpB2UVp3AeWM6ShosBNHBOfEDuIOKJvIC6hjrloBtpa6O3bcjBC39fV6cYuZ5WpdWMO4wPyBxvh6KiP7jeM3JwGFu3HNRWve1yLyyPpdT6B66R4_ePPsK_Gk1uhM_rADN0gdk-Aq3qH92c6MjUDDiQaRfOxM6mADN0i1IhYbmIA7yG.; isg=BFFRjvzi0yQ8Xz9aqLz2wdYwYFvrvsUwPYnpwjPmT5g32nEsew-tAPR7fa48VV1o'

    def __init__(self):
        super().__init__('damai')
        
    
    def crawl_origin_docs(self):
        self.exist_ids_list = self.get_exist_ids()

        params = {
            'keyword': '',
            'cty': '',
            'ctl': '演唱会',
            'sctl': '',
            'tsg': '0',
            'st': '',
            'et': '',
            'order': '3',
            'pageSize': '30',
            'currPage': '1',
            'tn': '',
        }
        update_id_list = []
        details_df = pd.read_csv('data/meta_info/damai.csv')
        df_ids = list(details_df['projectid'])

        response = requests.get(self.base_url, headers=self.headers, params=params)
        json_data = response.json()
        total_num_flag = 0
        while json_data['pageData']['currentPage'] < json_data['pageData']['totalPage']:
            num_flag = 0
            for item in json_data['pageData']['resultData']:
                id = item['projectid']
                if str(id) in self.exist_ids_list:
                    num_flag += 1
                    continue
                
                update_id_list.append(id)
                if id not in df_ids:
                    new_detail_row = {}
                    for col_ in details_df.columns:
                        new_detail_row[col_] = item[col_]
                    details_df = pd.concat([details_df, pd.DataFrame([new_detail_row])], ignore_index=True)
            print(f'Page {json_data["pageData"]["currentPage"]}, {num_flag} items has exist')
            if num_flag == json_data['pageData']['onePageSize']:
                total_num_flag += 1
                if total_num_flag > 5:
                    break
            params['currPage'] = str(int(params['currPage']) + 1)
            sleep(random.randint(1, 3))
            response = requests.get(self.base_url, headers=self.headers, params=params)
            json_data = response.json()

        print(f'Update {len(update_id_list)} new items')
        details_df.to_csv('data/meta_info/damai.csv', index=False)

        for id in update_id_list:
            json_docs = {}
            for col_ in details_df.columns:
                json_docs[col_] = str(details_df[details_df['projectid']==id][col_].values[0])
            
            detail_json = self.crawl_detail(id)

            json_docs.update(detail_json)
            # if f'{id}.json' not in os.listdir('data/origin_docs/damai/'):
            with open(f'data/origin_docs/damai/{id}.json', 'w') as f:
                json.dump(json_docs, f, indent=4, ensure_ascii=False)

            # self.unify_one_docs(id)

    def get_sign(self, ts, data, _m_h5_tk=None):
        if _m_h5_tk:
            token = _m_h5_tk.split('_')[0]
        else:
            token = "undefined"
        text = f'{token}&{ts}&12574478&{data}'
        md5 = hashlib.md5()
        md5.update(text.encode('utf-8'))
        result = md5.hexdigest()
        return result

    def crawl_detail(self, id):
        # 修改headers
        # 获取cookie
        response = requests.get('https://mtop.damai.cn/h5/mtop.damai.item.detail.getdetail/1.0/?jsv=2.7.2&appKey=12574478')
        r_cookies = requests.utils.dict_from_cookiejar(response.cookies)
        
        origin_cookie = self.cookie_template
        renew_cookie = origin_cookie.replace('_m_h5_tk=', '_m_h5_tk='+r_cookies['_m_h5_tk'])
        renew_cookie = renew_cookie.replace('_m_h5_tk_enc=', '_m_h5_tk_enc='+r_cookies['_m_h5_tk_enc'])
        headers = self.headers.copy()
        headers['Cookie'] = renew_cookie
        # 修改params
        params = {
            'jsv': '2.7.2',
            'appKey': '12574478',
            # 't': int(time.time()),
            't': 1715790992,
            'sign': '61c0dd04778bed4ea7d64fa94447b527',
            'api': 'mtop.damai.item.detail.getdetail',
            'v': '1.0',
            'H5Request': 'true',
            'type': 'json',
            'timeout': '10000',
            'dataType': 'json',
            'valueType': 'string',
            'forceAntiCreep': 'true',
            'AntiCreep': 'true',
            'useH5': 'true',
            'data': {
                "itemId":str(id),
                "platform":"8",
                "comboChannel":"2",
                "dmChannel":"damai@damaih5_h5"
            },
        }

        json_str = json.dumps(params['data'], ensure_ascii=False)
        json_str = json_str.replace(' ', '')
        params['data'] = json_str
        params['sign']= self.get_sign(ts = params['t'],
                    data = json_str,
                    _m_h5_tk=r_cookies['_m_h5_tk'],
                    )
        response = requests.get(self.detail_url, headers=headers, params=params)
        json_r = response.json()

        detail_json = {}
        if 'data' in json_r:
            if 'legacy' in json_r['data']:
                detail_json = json.loads(json_r['data']['legacy'])
            elif not isinstance(json_r['data'], str):
                detail_json = json_r['data']
            else:
                print(f'Error: {id}')
        else:
            print(f'Error: {id}')
        return detail_json 

    def unify_docs(self):
        origin_ids_list = self.get_origin_ids()
        exist_ids_list = self.get_exist_ids()
        for id in origin_ids_list:
            if id not in exist_ids_list:
                self.unify_one_docs(id)

    def unify_one_docs(self, origin_id):
        path = f'data/origin_docs/damai/{origin_id}.json'
        
        with open(path, 'r') as f:
            file_json = json.load(f)

            unified_json_docs = {
                "platform": "大麦",
                "project_id": file_json['projectid'],
                "project_name": file_json['name'],
                "category_name": file_json['categoryname'],
                "artists": [],  # 根据 dynamicExtData 设置
                "show_status": file_json['showstatus'],       #["PRESALE"?, 售票中] TODO 查看details_df的unique
                
                "show_time": file_json['showtime'], #"2024.06.15 周六 20:00"
                "session_time": [],
                "price": file_json['price_str'],

                "isGeneralAgent": "true" if file_json['isgeneralagent']=="1" else "false",  # 总票代
                "isHotProject": "false",
                "rating": None,

                "city_name": file_json['cityname'],
                "venue_name": file_json['venue'],
                "venue_info": {
                    "venue_address": None,
                    "lng": None,
                    "lat": None,
                },

                "project_info": "",         # 提取damai "itemExtendInfo" "itemExtend" HTML中的文本
                "project_imgs": [
                    file_json['verticalPic']
                ],
                "wantVO": {
                    "wantNum": None,
                    "wantNumStr": None,
                    "wantNumSuffix": None,
                    "wantDesc": None
                },
                "tours": [],
                "project_link": 'https://detail.damai.cn/item.htm?id=' + file_json['projectid'],
            }
            # 处理 artists 等
            print(f"Pre Processing {origin_id}")
            if "detailViewComponentMap" in file_json and "item" in file_json["detailViewComponentMap"]:
                item_json =  file_json["detailViewComponentMap"]["item"]
                if "staticData" in item_json:
                    static_data_json = item_json["staticData"]
                    if 'itemBase' in static_data_json and 'isHotProject' in static_data_json['itemBase']:
                        unified_json_docs['isHotProject'] = static_data_json['itemBase']['isHotProject']
                    ###########################################################################
                    if 'rating' in static_data_json and 'rating' in static_data_json['rating']:
                        unified_json_docs['rating'] = static_data_json['rating']['rating']
                    if 'venue' in static_data_json:
                        unified_json_docs['venue_info']['venue_address'] = static_data_json['venue']['venueAddr']
                        unified_json_docs['venue_info']['lng'] = static_data_json['venue']['lng']
                        unified_json_docs['venue_info']['lat'] = static_data_json['venue']['lat']
                    ###########################################################################
                    if 'itemExtendInfo' in static_data_json and 'itemExtend' in static_data_json['itemExtendInfo']:
                        item_html = static_data_json['itemExtendInfo']['itemExtend']
                        soup = BeautifulSoup(item_html, 'html.parser')
                        text_list = []
                        text_soup  = soup.find_all('p')
                        for p_ in text_soup:
                            text_list.append(p_.text)
                        unified_json_docs["project_info"] = "\n".join(text_list)

                        img_soup = soup.find_all('img')
                        for img_ in img_soup:
                            unified_json_docs["project_imgs"].append(img_.get('src'))
                    ###########################################################################
                    if 'tourProjects' in static_data_json:
                        for tour_item in static_data_json['tourProjects']:
                            unified_json_docs["tours"].append({
                                "itemId": tour_item['itemId'],
                                "cityName": tour_item['cityName'],
                                "showTime": tour_item['showTime']
                            })
                else:
                    print(f"staticData not in {origin_id}")
                ###########################################################################
                if "dynamicExtData" in item_json: # artists, wantVO, 
                    # unified_json_docs["artists"] = file_json['detailViewComponentMap']['itemExtendInfo']['itemExtend']
                    if 'artists' in item_json['dynamicExtData']:
                        for artist_item in item_json['dynamicExtData']['artists']:
                            unified_json_docs["artists"].append(artist_item['artistName'])
                    ###########################################################################
                    if 'wantVO' in item_json['dynamicExtData'] and 'wantNum' in item_json['dynamicExtData']['wantVO']:
                        unified_json_docs["wantVO"]["wantNum"] = item_json['dynamicExtData']['wantVO']['wantNum']
                        unified_json_docs["wantVO"]["wantNumStr"] = item_json['dynamicExtData']['wantVO']['wantNumStr']
                        unified_json_docs["wantVO"]["wantNumSuffix"] = item_json['dynamicExtData']['wantVO']['wantNumSuffix']
                        if 'wantDesc' in item_json['dynamicExtData']['wantVO']:
                            unified_json_docs["wantVO"]["wantDesc"] = item_json['dynamicExtData']['wantVO']['wantDesc']
                    ###########################################################################
                if 'item' in item_json and 'performBases' in item_json['item']:
                    for perform_item in item_json['item']['performBases']:
                        unified_json_docs["session_time"].append(perform_item['name'])
                else:
                    print(f"dynamicExtData not in {origin_id}")
            elif 'staticData' in file_json:
                if 'venue' in file_json['staticData']:
                    unified_json_docs['venue_info']['venue_address'] = file_json['staticData']['venue']['venueAddr']
                    unified_json_docs['venue_info']['lng'] = file_json['staticData']['venue']['lng']
                    unified_json_docs['venue_info']['lat'] = file_json['staticData']['venue']['lat']
                ###########################################################################
                if 'itemExtendInfo' in file_json['staticData'] and 'itemExtend' in file_json['staticData']['itemExtendInfo']:
                    item_html = file_json['staticData']['itemExtendInfo']['itemExtend']
                    soup = BeautifulSoup(item_html, 'html.parser')
                    text_list = []
                    text_soup  = soup.find_all('p')
                    for p_ in text_soup:
                        text_list.append(p_.text)
                    unified_json_docs["project_info"] = "\n".join(text_list)

                    img_soup = soup.find_all('img')
                    for img_ in img_soup:
                        unified_json_docs["project_imgs"].append(img_.get('src'))
            else:
                if 'actionControl' in file_json and 'hotProject' in file_json['actionControl']:
                    unified_json_docs['isHotProject'] = file_json['actionControl']['hotProject']
                ###########################################################################
                if 'venue' in file_json:
                    print()
                    unified_json_docs['venue_name'] = file_json['venue']['venueName']
                    unified_json_docs['venue_info']['venue_address'] = file_json['venue']['venueAddr']
                    unified_json_docs['venue_info']['lng'] = file_json['venue']['lng']
                    unified_json_docs['venue_info']['lat'] = file_json['venue']['lat']
                ###########################################################################
                if 'desc' in file_json:
                    item_html = file_json['desc']['introduce']
                    soup = BeautifulSoup(item_html, 'html.parser')
                    text_list = []
                    text_soup  = soup.find_all('p')
                    for p_ in text_soup:
                        text_list.append(p_.text)
                    unified_json_docs["project_info"] = "\n".join(text_list)

                    img_soup = soup.find_all('img')
                    for img_ in img_soup:
                        unified_json_docs["project_imgs"].append(img_.get('src'))
                ###########################################################################
                if 'guide' in file_json:
                    if 'artists' in file_json['guide']:
                        for artist_item in file_json['guide']['artists']:
                            unified_json_docs["artists"].append(artist_item['name'])
                    ###########################################################################
                    if 'tour' in file_json['guide'] and 'projectList' in file_json['guide']['tour']:
                        for tour_item in file_json['guide']['tour']['projectList']:
                            unified_json_docs["tours"].append({
                                "itemId": tour_item['itemId'],
                                "cityName": tour_item['cityName'],
                                "showTime": tour_item['showTime']
                            })
                    ###########################################################################
                    if 'want' in file_json['guide']:
                        unified_json_docs["wantVO"]["wantNum"] = file_json['guide']['want']['wantNum']
                        unified_json_docs["wantVO"]["wantNumStr"] = file_json['guide']['want']['wantNumStr']
                        unified_json_docs["wantVO"]["wantNumSuffix"] = file_json['guide']['want']['wantNumSuffix']
                        if 'wantDesc' in file_json['guide']['want']:
                            unified_json_docs["wantVO"]["wantDesc"] = file_json['guide']['want']['wantDesc']
                    ###########################################################################
            with open(f'data/unified_docs/damai/{origin_id}.json', 'w') as f:
                json.dump(unified_json_docs, f, ensure_ascii=False, indent=4)

    
if __name__ == '__main__':
    damai = DamaiCrawler()
    origin_ids_list = damai.get_origin_ids()
    print('origin_ids_list', len(origin_ids_list))
    exist_ids_list = damai.get_exist_ids()
    print('exist_ids_list', len(exist_ids_list))

    damai.crawl_origin_docs()
    damai.unify_docs()

    exist_ids_list = damai.get_exist_ids()
    print('exist_ids_list', len(exist_ids_list))

