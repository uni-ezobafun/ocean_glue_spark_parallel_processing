import traceback

from random import randint

from something_heavy_process.util import sort_by_bubble_sort

def process_in_non_parallel():
    random_list = [randint(0,100000) for _ in range(20000)]

    print(sort_by_bubble_sort(random_list))

if __name__ == "__main__":
    try:
        process_in_non_parallel()
    except:
        traceback.print_exc()