from glue_spark_test.something_heavy_process.util import sort_by_bubble_sort


def process_in_non_parallel(pre_process_data_queue):   
    whole_process_count = len(pre_process_data_queue)

    for index, pre_process_data in enumerate(pre_process_data_queue):
        print(sort_by_bubble_sort(pre_process_data))
        print(f"iteration_count: {index + 1}/{whole_process_count} completed")
