# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import pymongo
import mongoengine as db
import requests

from itemadapter import ItemAdapter
from datetime import datetime, timedelta
from . import items as it


class ContentItem(db.Document):
    view_count = db.IntField(default=0)
    click_count = db.IntField(default=0)
    created_at = db.DateTimeField(default=datetime.utcnow)
    updated_at = db.DateTimeField()
    invalid_at = db.DateTimeField()

    valid = db.BooleanField(default=True)

    meta = {
        'abstract': True
    }


class Price(db.EmbeddedDocument):
    original = db.IntField()
    sale = db.IntField()
    coupon = db.IntField()
    max = db.IntField()

    @property
    def is_empty(self):
        return self.original is None and self.sale is None and self.coupon is None and self.max is None

    def to_dict(self):
        return dict(self._data)


class Hotdeal(ContentItem):
    deal_srl = db.StringField(unique=True)
    link = db.URLField()
    title = db.StringField()
    image = db.URLField()
    price = db.EmbeddedDocumentField(Price)
    is_free_delivery = db.BooleanField()
    deal_class = db.StringField()
    market = db.StringField()
    momcha_category = db.StringField(default='MM_099')  # db.ReferenceField(MomchaCategory)
    market_category = db.StringField()
    market_rank = db.IntField(default=100)
    grade_score = db.DecimalField(precision=1)
    grade_count = db.IntField()
    sold_count = db.IntField()
    time_left = db.IntField()
    expired_at = db.DateTimeField()

    meta = {
        'allow_inheritance': True
    }


def post_hotdeal_keyword(hotdeal: Hotdeal):
    dev_url = 'http://13.125.225.106:5050/v1/noti/hotdeal'
    prod_url = 'http://momstouch-noti-prod.ap-northeast-2.elasticbeanstalk.com/v1/noti/hotdeal'

    _post_hotdeal_keyword_url(dev_url, hotdeal)
    _post_hotdeal_keyword_url(prod_url, hotdeal)


def _post_hotdeal_keyword_url(url: str, hotdeal: Hotdeal):
    requests.post(
        url=url,
        json={
            'hotdeal_title': hotdeal.title,
            'hotdeal_id': str(hotdeal.id),
            'hotdeal_image': hotdeal.image,
            'hotdeal_link': hotdeal.link
        }
    )


class HotdealPipeline:
    def __init__(self, mongo_uri):
        self.mongo_uri = mongo_uri
        self.alias = 'contents'
        self.client = None
        self.log_collection = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI')
        )

    def open_spider(self, spider):
        db.connect(host=self.mongo_uri,
                   db='contents',
                   alias=self.alias)
        self.client = pymongo.MongoClient(host=self.mongo_uri)
        self.log_collection = self.client.get_database('crawl').get_collection('hotdeal_record')

    def close_spider(self, spider):
        db.disconnect(alias=self.alias)
        self.client.close()

    def process_item(self, item, spider):
        return item


class TmonHotdeal(Hotdeal):
    market = db.StringField(default="tmon")

    meta = {
        'db_alias': 'contents'
    }


class TmonPipeline(HotdealPipeline):
    def process_item(self, item, spider):
        if not isinstance(item, it.ShoppingMallCrawlerItem):
            return item

        now = datetime.utcnow()
        deal_srl = f"{spider.name}:{item.get('deal_srl')}"
        old_tmon: TmonHotdeal = TmonHotdeal.objects(deal_srl=deal_srl).first()

        if old_tmon is None:
            tmon = TmonHotdeal()
        else:
            tmon = old_tmon

        tmon.deal_srl = deal_srl
        tmon.link = item.get('link')
        tmon.title = item.get('title')
        tmon.image = item.get('image')
        tmon.price = Price(original=item.get('original_price'),
                           sale=item.get('sale_price'),
                           coupon=item.get('coupon_price'),
                           max=item.get('max_price'))
        tmon.is_free_delivery = item.get('is_free_delivery')
        tmon.deal_class = '타임딜' if item.get('time_left') is not None else '핫딜'
        tmon.time_left = item.get('time_left')
        tmon.market_rank = item.get('rank', 100)
        tmon.momcha_category = item.get('mc_cat_code')
        tmon.market_category = item.get('category')
        tmon.grade_score = item.get('grade_score')
        tmon.grade_count = item.get('grade_count')
        tmon.sold_count = item.get('sold_count')
        tmon.expired_at = now + timedelta(seconds=item.get('time_left')) if item.get(
            'time_left') is not None else now + timedelta(
            hours=3)
        tmon.updated_at = now
        tmon.valid = not tmon.price.is_empty
        is_new = not tmon.id

        # with open('result.txt', 'w', encoding='utf-8') as f:
        #     print(
        #     f'Deal_srl: {tmon.deal_srl}',
        #     f'Link: {tmon.link}',
        #     f'Title: {tmon.title}',
        #     f'Image: {tmon.image}',
        #     f'Price: {tmon.price}',
        #     f'Delivery: {tmon.is_free_delivery}',
        #     f'Deal_class: {tmon.deal_class}',
        #     f'Time_left: {tmon.time_left}',
        #     f'Market_rank: {tmon.market_rank}',
        #     f'Momcha_category: {tmon.momcha_category}',
        #     f'Grade_score: {tmon.grade_score}',
        #     f'Grade_count: {tmon.grade_count}',
        #     f'Sold_count: {tmon.sold_count}',
        #     f'Expired_at: {tmon.expired_at}',
        #     f'Updated_at: {tmon.updated_at}',
        #     f'Valid: {tmon.valid}',
        #      'END', file=f, sep='\n')

        tmon.save()

        if is_new:
            post_hotdeal_keyword(tmon)

        log = dict(tmon._data)
        log['ref_id'] = log.pop('id')
        log['price'] = log.pop('price').to_dict()
        self.log_collection.insert_one(log)

        return item


class MongoPipeline:
    def __init__(self, mongo_uri):
        self.mongo_uri = mongo_uri

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client.get_database('tmon-database')

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        self.db.get_collection('temp_db').insert_one(ItemAdapter(item).asdict())

        return item
