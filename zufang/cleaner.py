import datetime

import pandas as pd


def clean(path):
    """
    执行数据清洗
    :param path:
    :return:
    """
    # 读取csv文件，转换为DataFrame
    reader = pd.read_csv(path,
                         header=0,
                         index_col=0,
                         na_values=['NULL', 'None', 'N/A', 'NA'],
                         names=['title', 'selling_point', 'price', 'area', 'house_type', 'floor', 'towards', 'metro',
                                'housing_estate',
                                'housing_estate_link', 'location', 'publish_at', 'broker', 'broker_homepage', 'number'],
                         chunksize=512)

    # 迭代数据块，执行清理操作
    for chunk in reader:
        # 1. 清除所有项为空的数据
        chunk.dropna(how='all', inplace=True)
        # 2. 清除重复数据，保留最后一条（这个明显有局限性，不能用于全局去重）
        chunk.drop_duplicates(keep='last', inplace=True)
        # 3. 清除租金和经纪人为空的数据
        chunk.dropna(subset=['price', 'broker'], inplace=True)
        # chunk.dropna(subset=['price', 'metro', 'selling_point'], inplace=True)

        # 将清洗过的数据保存下来（避免传输，可能整个数据文件会很大）
        save(chunk, path.replace('.csv', '-clean.csv'))


def save(chunk, target_path):
    """
    保存清洗过的数据
    :param chunk:
    :param target_path:
    :return:
    """

    # 写入时以追加方式写入（注意：index不能为False，否则将导致少写入第一列）
    chunk.to_csv(target_path, index=True, header=False, mode='a')


if __name__ == '__main__':
    clean(r'../.data/zufang-{}.csv'.format(datetime.date.today()))
