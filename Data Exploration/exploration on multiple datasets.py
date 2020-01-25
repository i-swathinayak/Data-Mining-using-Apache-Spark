from pyspark.shell import spark
import datetime
import sys
import json
import csv
from pyspark import SparkContext
import time

if __name__ == '__main__':
    input_file_1 = sys.argv[2]
    input_file_2 = sys.argv[1]
    output_file_1 = sys.argv[3]
    output_file_2 = sys.argv[4]

    multiple_data_task_3 = {}

    sc1 = SparkContext.getOrCreate()
    read_input = sc1.textFile(input_file_1)
    rdd1 = read_input.map(lambda x: json.loads(x))

    sc2 = SparkContext.getOrCreate()
    read_input = sc2.textFile(input_file_2)
    rdd2 = read_input.map(lambda x: json.loads(x))

    business_rdd = rdd1.map(lambda x: (x['business_id'], x['state']))
    review_rdd = rdd2.map(lambda x: (x['business_id'], x['stars']))
    stars_rdd = business_rdd.join(review_rdd).map(lambda x: (x[1][0], x[1][1]))

    aTuple = (0, 0)
    total_stars_rdd = stars_rdd.aggregateByKey(aTuple, lambda a, b: (a[0] + b, a[1] + 1), lambda a, b: (a[0] + b[0], a[1] + b[1]))
    avg_stars_rdd = total_stars_rdd.mapValues(lambda v: v[0] / v[1]).sortBy(lambda a: (-a[1],a[0]), ascending=True).collect()

    with open(output_file_1, 'w+') as out:
        csv_out = csv.writer(out)
        csv_out.writerow(['state', 'stars'])
        csv_out.writerows(avg_stars_rdd)

    # method1 using collect
    start_time = time.time()
    m1_value = total_stars_rdd.mapValues(lambda v: v[0] / v[1]).sortBy(lambda a: (-a[1], a[0]), ascending=True).map(lambda x: x[0]).collect()[:5]
    print(m1_value)
    end_time = time.time()
    collect_diff = end_time - start_time
    multiple_data_task_3["m1"] = collect_diff

    # method2 using take
    start_time = time.time()
    m2_value = total_stars_rdd.mapValues(lambda v: v[0] / v[1]).sortBy(lambda a: (-a[1], a[0]), ascending=True).map(lambda x: x[0]).take(5)
    print(m2_value)
    end_time = time.time()
    take_diff = end_time - start_time
    multiple_data_task_3["m2"] = take_diff

    time_diff = collect_diff - take_diff

    explanation = "When a collect operation is issued on a RDD, the entire dataset is copied to the the master node whereas take action retrieves only the specified number of elements. Hence, as can be seen from the example above, the execution time of 'collect' action is higher than that of 'take' action."

    multiple_data_task_3["explanation"] = explanation

    with open(output_file_2, 'w+') as outfile:
        json.dump(multiple_data_task_3, outfile)










