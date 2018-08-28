import datetime

import jieba
import pandas as pd
from jieba import analyse
from matplotlib import pyplot
from wordcloud import WordCloud

data_path = r'../.data/zufang-{}-clean.csv'.format(datetime.date.today())

# 使用Pandas读取数据，准备分析（这次一次读取，因为数据量并不大）
data = pd.read_csv(data_path,
                   header=0,
                   na_values=['NULL', 'None', 'N/A', 'NA'],
                   names=['title', 'selling_point', 'price', 'area', 'house_type', 'floor', 'towards', 'metro',
                          'housing_estate',
                          'housing_estate_link', 'location', 'publish_at', 'broker', 'broker_homepage', 'number'],
                   )

# 不要设置index_col=0参数，否则第一列（title）将被当成行索引
# RangeIndex(start=0, stop=2677, step=1)
print(data.index)
# Index(['title', 'selling_point', 'price', 'area', 'house_type', 'floor',
#        'towards', 'metro', 'housing_estate', 'housing_estate_link', 'location',
#        'publish_at', 'broker', 'broker_homepage', 'number'],
#       dtype='object')
print(data.columns)

# 由于卖点可能空，所以使用标题填充之
# 不能使用 inplace=True 参数，抛出：NotImplementedError异常
# 这里只取 title 和 selling_point 两列数据，用于生成词云
data = data.fillna(method='ffill', axis=1)[['title', 'selling_point']]

# 根据房屋标题和卖点生成词云
# 1. 将文本拼接起来，使用jieba分词器进行分词重组
text = ''
for item in data.values:
    text += ' '.join(item)

# 将一些比较中性或者无意义的词移除
[jieba.del_word(k) for k in [
    '看房', '小区', '入住', '此房', '号线', '一村', '家园',
    '出租', '可以', '一期', '二期', '精装修', '', '',
    '', '', '', '', '', '', '',
]]

# 增加一些词语
[jieba.add_word(k) for k in [
    '精装'
]]

# 进行中文分词，取Top100
tags = analyse.extract_tags(text, topK=100, withWeight=False)
text = ' '.join(tags)

# 对分词文本生成词云
# 生成词云，需要指定支持中文的字体，否则无法生成中文词云
wc = WordCloud(
    # 设置词云图片背景色，默认黑色
    # background_color='white',
    # 设置词云最大单词数
    max_words=200,
    # 设置词云中字号最大值
    # max_font_size=80,
    # 设置词云图片宽、高
    width=768,
    height=1024,
    # 设置词云文字字体(美化和解决中文乱码问题)
    font_path=r'../assets/fonts/FZXingKai-S04S.TTF'
).generate(text)

# 绘图(标准长方形图)
pyplot.imshow(wc, interpolation='bilinear')
pyplot.figure()
pyplot.axis('off')
# 将图片输出到文件
wc.to_file(r'../.data/zufang-{}-wc.png'.format(datetime.date.today()))

print('词云图片生成成功！')
