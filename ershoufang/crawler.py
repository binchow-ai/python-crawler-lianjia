"""
多进程爬虫，简单改造为多进程版本，还有较大优化空间
"""
import csv
import datetime
import logging
import multiprocessing as mp
import re
import time
from collections import OrderedDict
from multiprocessing.pool import Pool

import requests
from pyquery import PyQuery
from requests import RequestException

# 启动URL
start_url = r'https://sh.lianjia.com/ershoufang/'
# 任务URL列表，以列表页URL初始化之
new_urls = mp.Queue(1024 * 1024)
# 已处理URL集合没有很好的表示方法，这里使用普通集合+锁来实现多进程场景下应用
# 由于Queue无法判断元素是否存在，所以所有URL都使用该集合来判断
seen_urls = set()
lock = mp.Lock()
# 当前日期
today = datetime.date.today()

# 日志配置
logging.basicConfig(level=logging.INFO,
                    format='%(process)d - %(asctime)s - %(levelname)s - %(message)s')


def init():
    # 初始化任务队列
    new_urls.put(start_url)
    for i in range(2, 101):
        new_urls.put('https://sh.lianjia.com/ershoufang/pg{}/'.format(i))


def append_urls(urls):
    """
    管理URL，添加新的URL到任务队列中
    :param urls: URL列表
    :return:
    """
    with lock:
        for url in urls:
            if url not in seen_urls:
                new_urls.put(url)


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
            if text:
                with lock:
                    seen_urls.add(url)
            return text, url
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
    with lock:
        with open(r'../.data/ershoufang-{}.csv'.format(today),
                  'a', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(data.values())


def start(pool):
    """
    爬虫启动程序
    :param pool: 进程池对象
    :return:
    """
    i = 0
    while True:
        # 因为维护任务链接逻辑是多线程的，所以直接通过队列空来判断是不准确的
        # 这里当队列空时，程序休眠1秒钟，当计数（连续）超过60（一分钟）后，停止程序
        if new_urls.empty():
            if i >= 60:
                print('--exit--')
                break
            time.sleep(1)
            i += 1
        else:
            i = 0
            # 从队列中获取url，执行爬取操作
            new_url = new_urls.get()
            pool.apply_async(download, (new_url,), callback=handle)


def handle(args):
    # 下载失败时，返回空
    (html, url) = args
    if html is None:
        return
    try:
        # 解析网页（根据URL类型判断）
        if re.match('^https://sh.lianjia.com/ershoufang/(pg\d+/)?$', url):
            # 列表页
            links = parse_list_page(html, url)
            append_urls(links)
        elif re.match('^https://sh.lianjia.com/ershoufang/\d+.html$', url):
            # 详情页
            data = parse_item_page(html, url)
            # 将解析的数据保存下来
            if data is not None:
                process(data)
        else:
            # 不处理
            pass
    except Exception as e:
        logging.error(e)


if __name__ == '__main__':
    # 执行初始化函数
    init()

    # 构造一个进程池
    pool = Pool(mp.cpu_count())
    # 启动爬虫
    start(pool)
    # 关闭进程池
    pool.close()
    # 等待子进程完成
    pool.join()

    logging.info('--End--')
