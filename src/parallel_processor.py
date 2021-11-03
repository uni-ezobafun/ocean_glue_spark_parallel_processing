import traceback
from random import randint
from pyspark import SparkConf, SparkContext

# 用意するランダム値リストの要素数
# バブルソートの計算量はこの値の2乗
ITEM_COUNT_IN_A_DUMMY_LIST = 5000

# 用意するランダム値リストの個数
DUMMY_LIST_COUNT = 1000

# rddをいくつのgroupに分割するか (1000itemを200groupに分ける)
RDD_DIVISION_COUNT = 200


def main():
    pre_process_data_queue = create_dummy_data_queue()
    process_in_parallel(pre_process_data_queue)


def create_dummy_data_queue():
    dummy_data_queue = []
    for i in range(DUMMY_LIST_COUNT):
        dummy_data = [randint(0,100000) for _ in range(ITEM_COUNT_IN_A_DUMMY_LIST)]
        dummy_data_queue.append(dummy_data)

    return dummy_data_queue


def process_in_parallel(pre_process_data_queue):
    whole_process_count = len(pre_process_data_queue)

    rdd = organize_rdd(pre_process_data_queue)
    spark_artifacts = rdd.map(entry_spark_process).collect()

    for artifact in spark_artifacts:
        print(artifact["data"])


def organize_rdd(pre_process_data_queue):
    rdd_elements = []
    
    for index, pre_process_data in enumerate(pre_process_data_queue):
        new_tuple = (index, pre_process_data)
        rdd_elements.append(new_tuple)

    spark_context = SparkContext.getOrCreate()
    rdd = spark_context.parallelize(rdd_elements, RDD_DIVISION_COUNT)

    return rdd


def entry_spark_process(spark_arg_obj):
    index = spark_arg_obj[0]
    pre_process_data = spark_arg_obj[1]

    post_process_data = sort_by_bubble_sort(pre_process_data)

    return dict(
      {
        "index": index,
        "data": post_process_data
      }
    )


def sort_by_bubble_sort(num_list):
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