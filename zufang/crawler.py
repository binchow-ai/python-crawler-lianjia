# 爬虫主程序
import logging
import re
import time

import requests
from pyquery import PyQuery
from requests import RequestException

# 启动URL
start_url = r'https://sh.lianjia.com/zufang/'
# 任务URL列表，以列表页URL初始化之
new_urls = [start_url] + ['https://sh.lianjia.com/zufang/pg{}/'.format(i) for i in range(2, 101)]
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
    for li in pq('#house-lst > li div.info-panel > h2 > a').items():
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
            'selling_point': pq('div.content > div.title > div').text().strip(),
            'price': pq('div.price > span.total').text().strip(),
            'area': re.search('(\d+)', pq('div.zf-room > p:eq(0)').text()).group(1),
            'house_type': pq('div.zf-room > p:eq(1)').text().strip().replace('房屋户型：', ''),
            'floor': pq('div.zf-room > p:eq(2)').text().strip().replace('楼层：', ''),
            'towards': pq('div.zf-room > p:eq(3)').text().strip().replace('房屋朝向：', ''),
            'metro': pq('div.zf-room > p:eq(4)').text().strip().replace('地铁：', ''),
            'housing_estate': pq('div.zf-room > p:eq(5) > a:first').text().strip(),
            'housing_estate_link': pq('div.zf-room > p:eq(5) > a:first').attr('href'),
            'location': [i.text().strip() for i in pq('div.zf-room > p:eq(6) > a').items()],
            'publish_at': pq('div.zf-room > p:eq(7)').text().strip().replace('时间：', ''),
            'broker': pq('div.brokerName > a').text().strip(),
            'broker_homepage': pq('div.brokerName > a').attr('href'),
            'number': pq('span.houseNum').text().strip().replace('链家编号：', '')}

    return data


def process(data):
    """
    处理数据
    :param data:
    :return:
    """
    # TODO 数据清洗

    # TODO 数据存储

    print(data)
    pass


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
            if re.match('https://sh.lianjia.com/zufang/(pg\d+/)?', url):
                links = parse_list_page(html, url)
                append_urls(links)
            # 详情页
            if re.match('https://sh.lianjia.com/zufang/\d+.html', url):
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
    # url = 'https://sh.lianjia.com/zufang/107100478683.html'
    # html = download(url)
    # data = parse_item_page(html, url)
    # print(data)

    # 启动爬虫
    start()
