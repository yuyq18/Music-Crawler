import os

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
    
    def crawl_origin_docs(self):
        NotImplementedError
    
    def crawl_detail(self, id):
        NotImplementedError
    
    def unify_docs(self):
        NotImplementedError
    
    def unify_one_docs(self, origin_id):
        NotImplementedError