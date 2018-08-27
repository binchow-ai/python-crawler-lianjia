from collections import OrderedDict

# 测试有序字典，这里的有序指的是添加的顺序（而非按键排序）
dic = OrderedDict()
dic['name'] = 'Jane'
dic['age'] = 18
dic['gender'] = 'female'

# OrderedDict([('name', 'Jane'), ('age', 18), ('gender', 'female')])
print(dic)
# odict_keys(['name', 'age', 'gender'])
print(dic.keys())
# odict_values(['Jane', 18, 'female'])
print(dic.values())

# 修改和删除项
dic.update({'gender': '女'})
print(dic)
