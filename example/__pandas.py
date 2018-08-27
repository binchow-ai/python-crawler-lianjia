import pandas as pd
from pandas import DataFrame

# 读取csv文件，转换为DataFrame，这里采用分段读（文件可能比较大）
reader = pd.read_csv(r'../.data/zufang-2018-08-27.csv',
                     header=0,
                     index_col=0,
                     na_values=['NULL', 'None', 'N/A'],
                     names=['title', 'selling_point', 'price', 'area', 'house_type', 'floor', 'towards', 'metro',
                            'housing_estate',
                            'housing_estate_link', 'location', 'publish_at', 'broker', 'broker_homepage', 'number'],
                     chunksize=128)

# 迭代遍历数据块，分别做清洗操作
chunk = DataFrame()
for _ in reader:
    chunk = _

# print(chunk.T)

# 1. 清除所有项为空的数据
chunk.dropna(how='all', inplace=True)
# 2. 清除重复数据，保留最后一条
chunk.drop_duplicates(keep='last', inplace=True)
# 3. 清除租金为空的数据
chunk.dropna(subset=['price'], inplace=True)
# chunk.dropna(subset=['price', 'metro', 'selling_point'], inplace=True)

