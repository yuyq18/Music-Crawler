from Crawler.Damai import DamaiCrawler
from Crawler.Gewara import GewaraCrawler
from Crawler.Piaoxingqiu import PiaoxingqiuCrawler
    
if __name__ == '__main__':
    damai = DamaiCrawler()
    gewara = GewaraCrawler()
    piaoxingqiu = PiaoxingqiuCrawler()

    damai.crawl_origin_docs()
    damai.unify_docs()

    gewara.crawl_origin_docs()
    gewara.unify_docs()

    piaoxingqiu.crawl_origin_docs()
    piaoxingqiu.unify_docs()
    