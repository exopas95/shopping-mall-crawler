import scrapy
import time

from bs4 import BeautifulSoup
from scrapy_selenium import SeleniumRequest
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from itemloaders.processors import TakeFirst
from scrapy.loader import ItemLoader
from .. import items


class TmonSpider(scrapy.Spider):
    name = 'tmon'
    url = f'http://www.tmon.co.kr/deallist/54000000#strategyFilterNo=2,114,5,250,1'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15'}

    # ItemLoader 초기화??
    def __init__(self, category=None, *args, **kwargs):
        super(TmonSpider, self).__init__(*args, **kwargs)
        ItemLoader.default_output_processor = TakeFirst()

    def start_requests(self):
        yield self.create_req_tmon_li()

    def create_req_tmon_li(self):
        req = SeleniumRequest(
            url=self.url,
            headers=self.headers,
            wait_time=10,
            wait_until=EC.element_to_be_clickable((By.CLASS_NAME, 'anchor')),
            callback=self.parse_result,
            dont_filter=True
        )

        return req

    def parse_result(self, response):
        driver = response.request.meta['driver']
        self.sel_scroll_down_to_end(driver, max_count=1)

        source: str = driver.page_source
        soup = BeautifulSoup(source, 'html.parser')

        a_tags = soup.select('#_dealListContainer > li > a.anchor')
        links = [t['href'] for t in a_tags][:50]

        for i in range(len(links)):
            yield self.create_detail_req(links[i], i)

    def create_detail_req(self, url, i):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15'}

        req = SeleniumRequest(
            url=url,
            headers=headers,
            wait_time=10,
            wait_until=EC.element_to_be_clickable((By.CLASS_NAME, 'button_heart')),
            callback=self.parse_detail,
            dont_filter=True)

        req.meta['index'] = i

        return req

    def parse_detail(self, response):
        index = response.request.meta['index']
        l = ItemLoader(items.ShoppingMallCrawlerItem(), response=response)
        l.add_value('rank', index)
        l.add_xpath('deal_srl', '//meta[@property="og:url"]/@content')
        l.add_xpath('link', '//meta[@property="og:url"]/@content')
        l.add_xpath('image', '//meta[@property="og:image"]/@content')

        l.add_css('title',
                  '#view-default-scene-default > section.wrap_deals_basic.center_grid > div.bx_ct.deal_info > article.deal_info_summary > div.deal_title > h2')

        l.add_css('sale_price',
                  '#view-default-scene-default > section.wrap_deals_basic.center_grid > div.bx_ct.deal_info > article.deal_info_summary > div.deal_price > p.deal_price_sell > strong.number_unit')
        l.add_css('original_price',
                  '#view-default-scene-default > section.wrap_deals_basic.center_grid > div.bx_ct.deal_info > article.deal_info_summary > div.deal_price > div > span.deal_price_origin > del')
        l.add_css('coupon_price',
                  'span.partner_burden_price')
        l.add_css('is_free_delivery',
                  '#_dealInfoWrap > div > div.info_unit_contents > div > div > strong')

        l.add_css('grade_score',
                  '#_dealReviewWrap > div.deal_review > div > button > div > span.grade_average_score')
        l.add_css('grade_count',
                  '#_dealReviewWrap > div.deal_review > div > button > div > span.grade_average_count > span.num')
        l.add_css('sold_count',
                  '#view-default-scene-default > section.wrap_deals_basic.center_grid > div.bx_ct.deal_info > article.deal_info_summary > div.deal_price > p.deal_price_sell > span.deal_price_buy_count > span.number_unit')

        l.add_css('time_left', '#_dealInfoWrap > div > div.info_unit_contents > p > strong')

        l.add_css('category_tags',
                  '#view-default-scene-default > div.path-nav-wrap._fixedUIWrap._fixedTopMenu.fixed > div > div > div.category > a')

        detail = l.load_item()

        try:
            cats = detail.get('category_tags', [])
            if len(cats):
                detail['category'] = cats[-1]
        except:
            pass

        try:
            code, kor = items.map_momcha_category(detail['category_tags'])
            detail['mc_cat_code'] = code
        except:
            pass

        yield detail

    def sel_scroll_down_to_end(self, driver, max_count=3):
        scroll_pause_time = 0.5  # You can set your own pause time. My laptop is a bit slow so I use 1 sec
        screen_height = driver.execute_script("return window.screen.height;")  # get the screen height of the web
        i = 1

        while True:
            # scroll one screen height each time
            driver.execute_script("window.scrollTo(0, {screen_height}*{i});".format(screen_height=screen_height, i=i))
            i += 1
            time.sleep(scroll_pause_time)
            # update scroll height each time after scrolled, as the scroll height can change after we scrolled the page
            scroll_height = driver.execute_script("return document.body.scrollHeight;")
            # Break the loop when the height we need to scroll to is larger than the total scroll height
            if (screen_height) * i > scroll_height or i > max_count:
                break
