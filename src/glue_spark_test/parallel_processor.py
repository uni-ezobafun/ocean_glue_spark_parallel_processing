from random import randint
from pyspark import SparkConf, SparkContext

from glue_spark_test.something_heavy_process.util import sort_by_bubble_sort

# rddをいくつのgroupに分割するか (1000itemを200groupに分ける)
RDD_DIVISION_COUNT = 200


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
