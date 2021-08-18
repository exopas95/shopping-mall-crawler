from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from shopping_mall_crawler.spiders.tmon import TmonSpider


if __name__ == '__main__':
    runner = CrawlerProcess(get_project_settings())
    runner.crawl(TmonSpider)

    runner.start()
