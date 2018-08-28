import datetime
import re

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd

# 经纪人负责房屋数量统计
data_path = r'../.data/zufang-{}-clean.csv'.format(datetime.date.today())

# 使用Pandas读取数据，准备分析（这次一次读取，因为数据量并不大）
data = pd.read_csv(data_path,
                   header=0,
                   usecols=[12],
                   names=['broker'],
                   )

brokers = data['broker']
# print(brokers.value_counts())
# 问题：可以取得数值，但要怎样获取该值对应的索引呢？
# 何加琪    26
print(brokers.value_counts().max())
# 2677，个经纪人
print(brokers.count())
# 从数据项中提取第一级区域（注意实际是一个字符串而非元组）
r = brokers.value_counts()
# 只取管理房屋数大于的的（数量相对较少）
r = r[r > 5]
# 何加琪    26
# 李梦依     9
# 赖振辉     9
# 陈克胜     8
# 孙小霞     8
# 冯梦杰     8
# 吴琳飞     8
# 郭家俊     8
# 周毅      8
# 李奎      7
# 杨超      7
# 慈康      7
# 胡栋彬     7
# 胡浩      7
# 艾凤娇     6
# 李顺利     6
# 黄强      6
# 陈宏丽     6
# 谢鑫      6
# 李凯      6
# Name: broker, dtype: int64
print(r)

# 指定全局字体（默认：['sans-serif']），解决中文乱码问题，并统一全局字体
matplotlib.rcParams['font.family'] = ['SimHei']

# 生成条状图
plt.title('经纪人管理房屋数统计')
r.plot(kind='bar')
plt.grid(True, axis='y')
plt.show()
