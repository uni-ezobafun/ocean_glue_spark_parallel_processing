
from random import randint

# 用意するランダム値リストの要素数
# バブルソートの計算量はこの値の2乗
ITEM_COUNT_IN_A_DUMMY_LIST = 5000

# 用意するランダム値リストの個数
DUMMY_LIST_COUNT = 1000


def create_dummy_data_queue():
    dummy_data_queue = []
    for i in range(DUMMY_LIST_COUNT):
        dummy_data = [randint(0,100000) for _ in range(ITEM_COUNT_IN_A_DUMMY_LIST)]
        dummy_data_queue.append(dummy_data)

    return dummy_data_queue