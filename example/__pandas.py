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


# 数据填充测试
data = DataFrame({
    'A': [1, None],
    'B': [None, 2]
})

#      A    B
# 0  1.0  NaN
# 1  NaN  2.0
print(data)

# 测试填充一
# 按行填充，从上向下填充
#      A    B
# 0  1.0  NaN
# 1  1.0  2.0
print(data.fillna(method='ffill'))
# 按行填充，从下向上填充
#      A    B
# 0  1.0  2.0
# 1  NaN  2.0
print(data.fillna(method='bfill'))
# 按列填充，从左向右填充
#      A    B
# 0  1.0  1.0
# 1  NaN  2.0
print(data.fillna(method='ffill', axis=1))
# 按列填充，从右向左填充
#      A    B
# 0  1.0  NaN
# 1  2.0  2.0
print(data.fillna(method='bfill', axis=1))

data.fillna(method='ffill', inplace=True)
#      A    B
# 0  1.0  NaN
# 1  1.0  2.0
print(data)

data.fillna(method='ffill', inplace=True, axis=1)
#      A    B
# 0  1.0  1.0
# 1  1.0  2.0
print(data)

# 选取列
# 0    1.0
# 1    1.0
# Name: A, dtype: float64
print(data['A'])
# 选取多个列
#      A    B
# 0  1.0  1.0
# 1  1.0  2.0
print(data[['A', 'B']])

# 遍历pandas数据
for item in data.values:
    print(item)
