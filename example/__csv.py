import csv

# newline=''参数用于解决生成空行问题
with open('../.data/example.csv', 'w', encoding='utf-8', newline='') as f:
    writer = csv.writer(f)
    # 单行写入
    writer.writerow(('name', 'gender', 'birthday'))
    # 多行写入
    writer.writerows([('张三', '男', '1997-1-21'),
                      ('李四', '男', '1998-7-17'),
                      ('王五', '女', '1999-2-12')])

with open('../.data/example.csv', 'r', encoding='utf-8', newline='') as f:
    reader = csv.reader(f)
    for line in reader:
        print(line)
