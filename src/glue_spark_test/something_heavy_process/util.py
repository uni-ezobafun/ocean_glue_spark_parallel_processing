def sort_by_bubble_sort(num_list):
    for m in range(len(num_list)):
        for n in range(len(num_list)-1, m, -1):
            if num_list[n] < num_list[n-1]:
                num_list[n], num_list[n-1] = num_list[n-1], num_list[n]

    return num_list
