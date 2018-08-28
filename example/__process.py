import multiprocessing as mp

if __name__ == '__main__':
    queue = mp.Queue(4)

    # 判断队列空
    assert queue.empty()

    # 添加元素
    queue.put('Ashe')

    # 判断元素是否存在
    # TypeError: argument of type 'Queue' is not iterable
    # assert 'Ashe' in queue
