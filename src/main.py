import traceback

from dummy_data_creator import create_dummy_data_queue
from glue_spark_test.parallel_processor import process_in_parallel
from glue_spark_test.non_parallel_processor import process_in_non_parallel

# 分散処理させるか否か
IS_PARALLEL_PROCESSING = False


def main():
    pre_process_data_queue = create_dummy_data_queue()
    if IS_PARALLEL_PROCESSING:
        process_in_parallel(pre_process_data_queue)
    else:
        process_in_non_parallel(pre_process_data_queue)


if __name__ == "__main__":
    try:
        main()
    except:
        traceback.print_exc()
