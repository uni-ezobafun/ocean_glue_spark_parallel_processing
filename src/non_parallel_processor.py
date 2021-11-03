import traceback
from random import randint

# 用意するランダム値リストの要素数
# バブルソートの計算量はこの値の2乗
ITEM_COUNT_IN_A_DUMMY_LIST = 5000

# 用意するランダム値リストの個数
DUMMY_LIST_COUNT = 1000


def main():
    """AWS Glue Jobをキックして最初に呼ばれるエントリポイント 

    Args:

    Returns:

    """
    pre_process_data_queue = create_dummy_data_queue()
    process_in_non_parallel(pre_process_data_queue)


def create_dummy_data_queue():
    """ランダム値を規定の個数だけ詰めた2次元リストを返却(ここでは5000列 x 1000行)

    Args:

    Returns:
        dummy_data_queue: ランダム値の整数型リスト
    """
    dummy_data_queue = []
    for i in range(DUMMY_LIST_COUNT):
        dummy_data = [randint(0,100000) for _ in range(ITEM_COUNT_IN_A_DUMMY_LIST)]
        dummy_data_queue.append(dummy_data)

    return dummy_data_queue


def process_in_non_parallel(pre_process_data_queue):
    """分散処理なしで愚直にループを回して結果を標準出力

    Args:
        pre_process_data_queue: 変換前データ(ランダム値の整数型リスト)

    Returns:

    """
    whole_process_count = len(pre_process_data_queue)

    for index, pre_process_data in enumerate(pre_process_data_queue):
        print(sort_by_bubble_sort(pre_process_data))
        print(f"iteration_count: {index + 1}/{whole_process_count} completed")


def sort_by_bubble_sort(num_list):
    """重い処理の疑似としてバブルソート

    Args:
        num_list: ランダム値の整数型リスト

    Returns:
        num_list: 昇順ソート済の整数型リスト
    """
    for m in range(len(num_list)):
        for n in range(len(num_list)-1, m, -1):
            if num_list[n] < num_list[n-1]:
                num_list[n], num_list[n-1] = num_list[n-1], num_list[n]

    return num_list


if __name__ == "__main__":
    try:
        main()
    except:
        traceback.print_exc()
