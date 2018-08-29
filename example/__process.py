import csv
import multiprocessing as mp


def write_to_file(msg):
    with open(r'../.data/__process.csv',
              'a', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(('msg', msg))


if __name__ == '__main__':
    queue = mp.Queue(4)

    # 判断队列空
    assert queue.empty()

    # 添加元素
    queue.put('Ashe')

    # 判断元素是否存在
    # TypeError: argument of type 'Queue' is not iterable
    # assert 'Ashe' in queue

    # 测试多进程写入同一文件
    pool = mp.Pool(4)

    # 多进程写入同一文件（测试结论：顺序会乱，但不会相互串掉，所以可以并行写入）
    for i in range(1024):
        pool.apply_async(write_to_file, ('hello-{:04d}'.format(i + 1),))

    pool.close()
    pool.join()
