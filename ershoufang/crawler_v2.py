"""
多进程爬虫，第二版

1. 将爬虫分解为下载任务和解析任务（可以继续分解，但在本案中意义不大）两部分，两部分各使用一个子进程，相互通过数据管道通信
2. 下载任务内部不使用队列，使用任务管道实现（在多进程：主进程、子进程、子进程内部进程池等场景下，队列并不好用）任务管理和通信
3. 解析任务从与下载任务间的管道中获取数据，解析并保存

问题：当目标被爬完后，怎样让爬虫停止？
"""
import csv
import datetime
import logging
import multiprocessing as mp
import re
import time
from collections import OrderedDict

import requests
from pyquery import PyQuery
from requests import RequestException

base_url = r'https://sh.lianjia.com/ershoufang'
# 已处理URL集合没有很好的表示方法，这里使用普通集合+锁来实现多进程场景下应用
seen_urls = set()
lock = mp.Lock()
# 下载失败重试次数
retries = 3
# 当前日期
today = datetime.date.today()

# 列表页、明细页URL正则表达式
list_page_pattern = '^{}/(pg\d+/)?$'.format(base_url)
item_page_pattern = '^{}/\d+.html$'.format(base_url)

# 数据存储路径
csv_file_path = r'../.data/ershoufang-{}.csv'.format(today)

# 日志配置
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(process)05d - %(levelname)s - %(message)s')


def start_download_job(data_writer, init_tasks):
    """
    下载任务（作业）
    :param data_writer: 数据管道（写）
    :param init_tasks: 初始任务集合
    :return:
    """
    # 构造进程池，按CPU核数初始化进程池大小，小于4核以4为准，否则以CPU核数为准
    pool_size = mp.cpu_count() > 4 and mp.cpu_count() or 4
    pool = mp.Pool(pool_size)

    # 任务不使用队列（在这种进程中使用子进程和进程池的应用中，队列会遇到各种问题），使用管道实现
    (task_reader, task_writer) = mp.Pipe(duplex=False)

    # 为了简化代码，初始任务直接通过任务管道发送出去，再接收
    # 也可以直接在循环代码中实现，当初始任务集合为空时，再使用任务管道接收任务
    task_writer.send(init_tasks)

    # 循环从任务管道中读取任务数据，并进行处理
    while True:
        # 任务是一组URL
        urls = task_reader.recv()
        # 使用进程池，分别下载这些URL，将下载后的文档内容和url构成的元组通过管道发出
        for url in urls:
            # 判断任务是否重复
            with lock:
                if url in seen_urls:
                    continue
                else:
                    seen_urls.add(url)
            # 执行下载任务
            pool.apply_async(download, (url, task_writer, data_writer))

    pool.close()
    pool.join()


def download(url, task_writer, data_writer):
    """
    下载网页，最多重试3次
    :param url: 下载url地址
    :param task_writer: 任务管道（写）
    :param data_writer: 数据管道（写）
    :return:
    """
    for _ in range(retries + 1):
        try:
            logging.info('download page {}'.format(url))
            content = requests.get(url).text
            if content is None:
                continue
            # 抽取列表页的中链接列表
            if is_list_page(url):
                links = parse_list_page(content, url)
                # 将详情页链接列表通过管道发出去
                if links and len(links) > 0:
                    task_writer.send(links)
            else:
                data_writer.send((content, url))
            return
        except RequestException:
            # 异常时休眠2秒
            time.sleep(2)
    # 超过重试次数则打印错误消息
    logging.error('重试{}次下载仍失败：{}'.format(retries, url))
    # 将失败url重新加入任务队列
    task_writer.send(set([url]))


def is_list_page(url):
    """
    判断是否列表页
    :param url:
    :return:
    """
    return re.match(list_page_pattern, url)


def parse_list_page(content, url):
    """
    列表网页解析器
    :param content:
    :param url:
    :return: 详情页链接集合
    """
    pq = PyQuery(content, url=url)
    return set([li.attr('href') for li in pq('ul.sellListContent div.title > a').items()])


def parse_item_page(content, url):
    """
    详情页解析器
    :param content:
    :param url:
    :return: 返回详情数据
    """
    pq = PyQuery(content, url=url)
    return OrderedDict({'title': pq('div.content > div.title > h1').text().strip(),
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
                        'number': pq('div.houseRecord > span.info').text().replace('举报', '').strip()})


def start_parse_job(data_reader):
    """
    解析任务（作业）
    :param data_reader: 数据管道（读）
    :return:
    """
    # 构造进程池，按CPU核数初始化进程池大小，小于4核以4为准，否则以CPU核数为准
    pool_size = mp.cpu_count() > 4 and mp.cpu_count() or 4
    # 解析任务只使用下载任务进程池规模的一半（视情况而定，目前其处理速度要远大于下载任务，也避免进程过多）
    pool = mp.Pool(pool_size // 2)
    while True:
        args = data_reader.recv()
        if args is not None:
            pool.apply_async(parse, args, callback=process)

    pool.close()
    pool.join()


def parse(content, url):
    """
    解析网页
    :param content:
    :param url:
    :return:
    """
    if content is None or url is None:
        return
    try:
        # 解析详情页，返回数据
        return parse_item_page(content, url)
    except Exception as e:
        logging.error(e)


def process(data):
    """
    处理数据
    :param data:
    :return:
    """
    if data is None:
        return
        # 数据基本处理
    # 处理小区链接不完整问题
    if 'housing_estate_link' in data and not data['housing_estate_link'].startswith('https://'):
        data['housing_estate_link'] = 'https://sh.lianjia.com' + data['housing_estate_link']

    # 数据转换
    # 提取户型中的室数
    if 'house_type' in data:
        data['house_type'] = (data['house_type'].split('室')[0], data['house_type'])

    # 数据存储（写入CSV文件，文件按日期生成）
    with open(csv_file_path,
              'a', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(data.values())


if __name__ == '__main__':
    # 初始任务集合
    init_tasks = set([base_url + '/'] + ['{}/pg{}/'.format(base_url, i) for i in range(2, 101)])

    # 创建管道，用于任务（进程）间通信
    (data_reader, data_writer) = mp.Pipe(duplex=False)

    # 启动下载任务（写端）
    mp.Process(target=start_download_job, args=(data_writer, init_tasks)).start()

    # 启动解析任务（读端）
    mp.Process(target=start_parse_job, args=(data_reader,)).start()

    logging.info('--running--')
