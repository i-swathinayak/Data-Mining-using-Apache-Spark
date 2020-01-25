from pyspark import SparkContext
from itertools import combinations
import time
import csv
import sys
import os


def local_frequent_items(baskets):
    local_baskets = list(baskets)
    partition_support = support * (len(local_baskets)/total_baskets)
    partition_result = []
    true_freq_items = []
    items_counter = {}

    for basket in local_baskets:
        for item in basket:
            items_counter[item] = items_counter.get(item, 0) + 1

    for item, count in items_counter.items():
        if count >= partition_support:
            true_freq_items.append(item)
    true_freq_items = sorted(true_freq_items)

    single_items = [(x,) for x in true_freq_items]
    partition_result.extend(single_items)

    filter_freq_items = set(true_freq_items)

    for size in range(2, len(filter_freq_items)):
        count = len(filter_freq_items)
        if count == 0:
            break

        candidate_items = []
        true_freq_items = []
        items_counter = {}

        if size == 2:
            for pair in combinations(filter_freq_items, 2):
                candidate_items.append(sorted(pair))
        else:
            filter_freq_items = list(filter_freq_items)
            for i in range(count - 1):
                for j in range(i + 1, count):
                    item1 = filter_freq_items[i]
                    item2 = filter_freq_items[j]
                    if item1[0:(size - 2)] == item2[0:(size - 2)]:
                        candidate_items.append(list(set(item1).union(set(item2))))
                    else:
                        break


        for candidate in candidate_items:
            scandidate = set(candidate)
            tcandidate = tuple(sorted(candidate))
            for basket in local_baskets:
                if scandidate.issubset(basket):
                    items_counter[tcandidate] = items_counter.get(tcandidate, 0) + 1

        for candidate_item, count in items_counter.items():
            if count >= partition_support:
                true_freq_items.append(candidate_item)

        true_freq_items = sorted(true_freq_items)
        partition_result.extend(true_freq_items)
        filter_freq_items = true_freq_items[:]

    return partition_result


def global_frequent_items(baskets):
    counter = {}
    for item in local_freq_items:
        counter[item] = 0

    for basket in baskets:
        for item in counter:
            scandidate = set(item)
            if scandidate.issubset(basket):
                counter[item] += 1

    return counter.items()


def write_to_file(result_set, count):
    len_1 = len(result_set[0])
    data = str(result_set[0]).replace(',', '')
    file.write(data)
    for k in range(1, count):
        len_2 = len(result_set[k])
        if len_1 == len_2:
            file.write(", ")
        else:
            file.write("\n\n")

        if len_2 == 1:
            data = str(result_set[k]).replace(',', '')
        else:
            data = str(result_set[k])
        file.write(data)
        len_1 = len_2


if __name__ == '__main__':
    start_time = time.time()

    threshold = int(sys.argv[1])
    support = int(sys.argv[2])
    input_file=sys.argv[3]
    output_file=sys.argv[4]
    interim_file = os.path.dirname(output_file) + '/customer_product.csv'

    sc = SparkContext.getOrCreate()
    raw_data = sc.textFile(input_file)

    header = raw_data.first()

    baskets = raw_data.filter(lambda line: line != header).map(lambda line: (str(line.split(",")[0].replace('\"', '')) + "-" + str(line.split(",")[1].replace('\"', '')),int(str(line.split(",")[5]).replace('\"', '')))).collect()

    with open(interim_file, 'w+') as out:
        csv_out = csv.writer(out, quoting=csv.QUOTE_NONE, quotechar='', escapechar='\\')
        csv_out.writerow(['DATE-CUSTOMER_ID', 'PRODUCT_ID'])
        csv_out.writerows(baskets)

    data = sc.textFile(interim_file)

    fileheader = data.first()

    baskets = data.filter(lambda line: line != fileheader).map(lambda line: line.split(",")).map(lambda line: (str(line[0]), int(line[1])))
    filtered_user_baskets = baskets.groupByKey().mapValues(set).values().filter(lambda x: len(x) > threshold)
    total_baskets = filtered_user_baskets.count()
    # print(total_baskets)

    # Phase 1
    local_freq_items = filtered_user_baskets.mapPartitions(local_frequent_items).map(lambda x: (x,1)).reduceByKey(lambda x,y: 1).keys().collect()
    # print(len(local_freq_items))

    # Phase 2
    global_freq_items = filtered_user_baskets.mapPartitions(global_frequent_items).reduceByKey(lambda x,y: x+y).filter(lambda x: x[1] >= support)
    frequentitems = global_freq_items.keys().collect()
    print(len(frequentitems))

    candidate_result_set = sorted(local_freq_items, key=lambda x: (len(x), x))
    count_1 = len(candidate_result_set)

    result_set = sorted(frequentitems, key=lambda x: (len(x), x))
    count_2 = len(result_set)

    candidate_result_set_print = list()
    for item_set in candidate_result_set:
        temp = []
        for item in item_set:
            temp.append(str(item))
        temp = tuple(temp)
        candidate_result_set_print.append(temp)

    # print(candidate_result_set_print)

    result_set_print = list()
    for item_set in result_set:
        temp = []
        for item in item_set:
            temp.append(str(item))
        temp = tuple(temp)
        result_set_print.append(temp)


    file = open(output_file, "w")

    header_1 = "Candidates: \n"
    file.write(header_1)

    if count_1 != 0:
        write_to_file(candidate_result_set_print, count_1)

    file.write("\n\n")
    header_2 = "Frequent Itemsets: \n"
    file.write(header_2)

    if count_2 != 0:
        write_to_file(result_set_print, count_2)

    file.close()

    end_time = time.time()
    print("Duration: ", end_time - start_time, " seconds")




