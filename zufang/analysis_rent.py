import datetime
import matplotlib
import matplotlib.pyplot as plt
import pandas as pd

data_path = r'../.data/zufang-{}-clean.csv'.format(datetime.date.today())

# 使用Pandas读取数据，准备分析（这次一次读取，因为数据量并不大）
# 使用 usecols=[2, 14] 参数控制只读取租金和编号（非必要）两列
data = pd.read_csv(data_path,
                   header=0,
                   usecols=[2, 3, 14],
                   names=['price', 'area', 'number'],
                   )

# 统计租金区间分布
prices = data['price']

# 清除大于50,000的（太离谱了，对普通用户没有参考价值）
# prices = prices[prices < 50000]

# 最大值、平均值、最小值
# 租金最高：350000.00 元/月
print('租金最高：{:.2f} 元/月'.format(prices.max()))
# 租金平均：11663.35 元/月
print('租金平均：{:.2f} 元/月'.format(prices.mean()))
# 租金最低：1000.00 元/月
print('租金最低：{:.2f} 元/月'.format(prices.min()))

# 统计每平米租金，并生成一个新列
data['per_square_meter'] = data['price'] / data['area']
# print(data)

# 自动分10段，但通常没有太大意义
# cuts = pd.cut(data['per_square_meter'], 10)
# 自行指定分段范围
cuts = pd.cut(data['per_square_meter'], [0, 20, 40, 60, 80, 100, 120, 140, 160, 180, 200, 500])
# <class 'pandas.core.series.Series'>
print(type(cuts))

# 统计价格区间内的房屋数（这里参与计算的字段可以使用任意字段，因为只是用于分组计数而已）
r = data['number'].groupby(cuts).count()
# per_square_meter
# (0, 20]        51
# (20, 40]      312
# (40, 60]      437
# (60, 80]      465
# (80, 100]     465
# (100, 120]    325
# (120, 140]    218
# (140, 160]    151
# (160, 180]     95
# (180, 200]     66
# (200, 500]     90
# Name: number, dtype: int64
print(r)

# 指定全局字体（默认：['sans-serif']），解决中文乱码问题，并统一全局字体
matplotlib.rcParams['font.family'] = ['SimHei']

# 生成条状图
plt.title('每平米房租价格区间分布图')
r.plot(kind='bar')
plt.grid(True, axis='y')
plt.show()

# 生成饼图
plt.title('每平米房租价格区间分布图')
r.plot(kind='pie', figsize=(6, 6), autopct='%1.1f%%')
plt.legend()
plt.show()
