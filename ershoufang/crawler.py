"""
第一版：单进程二手房信息爬虫
"""
import csv
import datetime
import logging
import re
import time
from collections import OrderedDict

import requests
from pyquery import PyQuery
from requests import RequestException

# 启动URL
start_url = r'https://sh.lianjia.com/ershoufang/'
# 任务URL列表，以列表页URL初始化之
new_urls = [start_url] + ['https://sh.lianjia.com/ershoufang/pg{}/'.format(i) for i in range(2, 101)]
seen_urls = set()

# 日志配置
logging.basicConfig(level=logging.WARN)


def append_urls(urls):
    """
    管理URL，添加新的URL到任务队列中
    :param urls: URL列表
    :return:
    """
    [new_urls.append(url) for url in urls if url not in new_urls and url not in seen_urls]


def download(url):
    """
    网页下载器
    :param url:
    :return:
    """
    for _ in range(4):
        try:
            logging.info('download page {}'.format(url))
            text = requests.get(url).text
            # 将处理过的URL添加到集合中
            seen_urls.add(url)
            return text
        except RequestException as e:
            # 异常时休眠2秒
            time.sleep(2)
    # 最多重试3次，停止重试，打印日志
    logging.error('重试3次下载仍失败URL：{}'.format(url))


def parse_list_page(html, url):
    """
    列表网页解析器
    :param html:
    :param url:
    :return: 详情页链接集合
    """
    pq = PyQuery(html, url=url)

    lst = []
    for li in pq('ul.sellListContent div.title > a').items():
        lst.append(li.attr('href'))

    return set(lst)


def parse_item_page(html, url):
    """
    详情页解析器
    :param html:
    :param url:
    :return: 返回详情数据
    """

    pq = PyQuery(html)

    data = {'title': pq('div.content > div.title > h1').text().strip(),
            'sub_title': pq('div.content > div.title > div.sub').text().strip(),
            'price': pq('div.price > span.total').text().strip(),
            'unit_price': pq('div.unitPrice > span.unitPriceValue').text().replace('元/平米', '').strip(),
            'down_payment_info': pq('div.tax > span.taxtext').text().strip(),
            'area': re.search('(\d+\.?\d*)', pq('div.area > div.mainInfo').text()).group(1),
            'year_info': pq('div.area > div.subInfo').text().strip(),
            'house_type': pq('div.room > div.mainInfo').text().strip(),
            'floor': pq('div.room > div.subInfo').text().strip(),
            'towards': pq('div.type > div.mainInfo').text().strip(),
            'housing_estate': pq('div.communityName > a:first').text().strip(),
            'housing_estate_link': pq('div.communityName > a:first').attr('href'),
            'location': tuple([i.text().strip() for i in pq('div.areaName > span > a').items()]),
            'broker': pq('div.brokerName > a').text().strip(),
            'broker_homepage': pq('div.brokerName > a').attr('href'),
            'number': pq('div.houseRecord > span.info').text().replace('举报', '').strip()}

    return OrderedDict(data)


def process(data):
    """
    处理数据
    :param data:
    :return:
    """
    # 数据基本处理
    # 处理小区链接不完整问题
    if 'housing_estate_link' in data and not data['housing_estate_link'].startswith('https://'):
        data['housing_estate_link'] = 'https://sh.lianjia.com' + data['housing_estate_link']

    # 数据转换
    # 提取户型中的室数
    if 'house_type' in data:
        data['house_type'] = (data['house_type'].split('室')[0], data['house_type'])

    # 数据存储（写入CSV文件，文件按日期生成）
    with open(r'../.data/ershoufang-{}.csv'.format(datetime.date.today()),
              'a', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(data.values())


def start():
    """
    爬虫启动程序
    :return:
    """
    while new_urls:
        try:
            # 从任务列表中取出URL
            url = new_urls.pop()
            # 下载网页
            html = download(url)
            # 下载失败时，返回空
            if html is None:
                continue
            # 解析网页（根据URL类型判断）
            # 列表页
            if re.match('https://sh.lianjia.com/ershoufang/(pg\d+/)?', url):
                links = parse_list_page(html, url)
                append_urls(links)
            # 详情页
            if re.match('https://sh.lianjia.com/ershoufang/\d+.html', url):
                data = parse_item_page(html, url)
                # 将解析的数据保存下来
                if data is not None:
                    process(data)
        except Exception as e:
            print(e)


if __name__ == '__main__':
    # # 测试列表解析器
    # url = start_url
    # html = download(url)
    # links = parse_list_page(html, url)
    # print(links)

    # # 测试详情页解析器
    # url = 'https://sh.lianjia.com/ershoufang/107100137964.html'
    # html = download(url)
    # data = parse_item_page(html, url)
    #
    # # 测试数据处理器
    # process(data)

    # 启动爬虫
    start()
