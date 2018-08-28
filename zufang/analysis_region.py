import datetime
import re

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd

# 房屋区域分布统计
data_path = r'../.data/zufang-{}-clean.csv'.format(datetime.date.today())

# 使用Pandas读取数据，准备分析（这次一次读取，因为数据量并不大）
data = pd.read_csv(data_path,
                   header=0,
                   usecols=[8, 10],
                   names=['housing_estate', 'location'],
                   )

# 最多的区域（具体到二级）
locations = data['location']
# print(locations.value_counts())
# 问题：可以取得数值，但要怎样获取该值对应的索引呢？
# ('青浦', '徐泾')       104
print(locations.value_counts().max())

# 房屋最多的小区
housing_estates = data['housing_estate']
# print(housing_estates.value_counts())
# 问题：可以取得数值，但要怎样获取该值对应的索引呢？
# 广东发展银行大厦       25
print(housing_estates.value_counts().max())

# 区域分布（由于具体到二级区域会变得非常多，所以只取第一级）
# ['', '', '浦东', '', ' ', '源深', '', '']
# print(re.split("[()',]", "('浦东', '源深')"))
# 从数据项中提取第一级区域（注意实际是一个字符串而非元组）
r = data['location'].map(lambda x: re.split("[()',]", x)[2]).value_counts()
# 浦东      727
# 闵行      304
# 松江      271
# 青浦      183
# 徐汇      180
# 长宁      165
# 黄浦      135
# 嘉定      127
# 普陀      113
# 宝山       96
# 杨浦       93
# 静安       76
# 闸北       73
# 虹口       73
# 奉贤       57
# 上海周边      2
# 金山        2
# Name: location, dtype: int64
print(r)

# 指定全局字体（默认：['sans-serif']），解决中文乱码问题，并统一全局字体
matplotlib.rcParams['font.family'] = ['SimHei']

# 生成条状图
plt.title('房屋区域分布')
r.plot(kind='bar')
plt.grid(True, axis='y')
plt.show()

# 生成饼图
plt.title('房屋区域分布')
r.plot(kind='pie', figsize=(6, 6), autopct='%1.1f%%')
plt.legend()
plt.show()
