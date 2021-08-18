# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html
import scrapy

from itemadapter import ItemAdapter
from scrapy.item import Field
from typing import Optional, List
from itemloaders.processors import MapCompose, Identity
from w3lib.html import remove_tags
import html

tmon_cats_2 = [t.split('\t') for t in """분유·유아식품	MM_006
수유·이유용품	MM_007
위생·건강·세제	MM_013
유아목욕·스킨케어	MM_012
안전·실내용품	MM_011
유아동침구·가구	MM_011
유아동잡화	MM_003
임부·태교용품	MM_016
임부패션	MM_016
출산준비·돌·선물	MM_015
완구·교구·도서	MM_014""".splitlines()]

tmon_cats_2_dict = {k: v for k, v in tmon_cats_2}

tmon_cats_3 = [t.split('\t') for t in """기저귀	MM_004
물티슈	MM_005
유아 건티슈	MM_005
물티슈캡	MM_005
물티슈액세서리	MM_005
유모차·용품	MM_008
카시트·용품	MM_009
아기띠·힙시트	MM_010
기저귀가방·파우치	MM_010
미아방지·기타용품	MM_011
신생아의류	MM_001
유아의류	MM_002
아동의류	MM_002
주니어의류	MM_002
내의·언더웨어	MM_002
시즌·이벤트의류	MM_002
브랜드의류	MM_002""".splitlines()]

tmon_cats_3_dict = {k: v for k, v in tmon_cats_3}

momcha_categories = [t.split('\t') for t in \
                     """MM_001	베이비패션
MM_002	키즈패션
MM_003	유아동잡화
MM_004	기저귀
MM_005	물티슈
MM_006	분유·이유식·식품
MM_007	수유/이유용품
MM_008	유모차·웨건
MM_009	카시트
MM_010	아기띠·외출용품
MM_011	침구·가구·안전용품
MM_012	목욕·스킨케어
MM_013	위생·건강·세제
MM_014	완구·교구·도서
MM_015	출산준비물·선물
MM_016	임부·태교용품
MM_099	기타""".splitlines()]

momcha_categories_dict = {k: v for k, v in momcha_categories}


def map_momcha_category(category_tags):
    second = category_tags[1]
    third = category_tags[2]

    if second in tmon_cats_2_dict:
        key = tmon_cats_2_dict.get(second, "MM_099")
    else:
        key = tmon_cats_3_dict.get(third, "MM_099")

    return (key, momcha_categories_dict[key])


def split_deal_srl(value):
    if isinstance(value, str) and '/' in value:
        return value.split('/')[-1]


def extract_delivery_price(value):
    if '무료배송' in value:
        return 0
    else:
        return to_int(value)


def extract_time_left_tmon(value):
    text = value.split("\n")[0]

    if '일 ' in text:
        day, left = text.split('일')

        return int(day) * 3600 * 24 + extract_time_left(text)
    else:
        return extract_time_left(text)


def extract_time_left(value):
    try:
        hours, minutes, seconds = [int(''.join(filter(str.isdigit, s))) for s in value.split(':')]
        return hours * 3600 + minutes * 60 + seconds
    except:
        return None


def to_int(s):
    try:
        filtered = ''.join(filter(str.isdigit, s.split()[0]))
        return int(filtered)
    except:
        return None


def to_float(s):
    try:
        return float(s)
    except:
        return None

def to_str(s):
    try:
        filtered = s.strip().split("\n")[0]
        return filtered
    except:
        return None


class ShoppingMallCrawlerItem(scrapy.Item):
    deal_srl: str = Field(input_processor=MapCompose(remove_tags, split_deal_srl))
    link: str = Field()
    title: str = Field(input_processor=MapCompose(remove_tags, html.unescape, to_str))
    image: str = Field()

    sale_price: int = Field(input_processor=MapCompose(remove_tags, to_int))
    original_price: Optional[int] = Field(input_processor=MapCompose(remove_tags, to_int))
    coupon_price: Optional[int] = Field(
        input_processor=MapCompose(remove_tags, lambda x: x if '할인가' in x else None, to_int))
    max_price: Optional[int] = Field()

    is_free_delivery: bool = Field(default=False,
                                   input_processor=MapCompose(remove_tags, lambda x: '무료배송' in x))

    grade_score: float = Field(input_processor=MapCompose(remove_tags, to_float))
    grade_count: int = Field(input_processor=MapCompose(remove_tags, to_int))
    sold_count: int = Field(input_processor=MapCompose(remove_tags, to_int))

    time_left: Optional[int] = Field(input_processor=MapCompose(remove_tags, extract_time_left_tmon))
    category_tags: List[str] = Field(input_processor=MapCompose(remove_tags, to_str), output_processor=Identity())
    category: str = Field()
    mc_cat_code: Optional[str] = Field()

    rank: int = Field()
