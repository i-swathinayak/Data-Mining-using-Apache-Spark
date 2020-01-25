from pyspark.shell import spark
import time
import json
import sys
from pyspark import SparkContext

if __name__ == '__main__':
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    n_partitions = int(sys.argv[3])

    partition_task2 = {}

    sc = SparkContext.getOrCreate()
    read_input = sc.textFile(input_file)
    usersrdd = read_input.map(lambda x: json.loads(x))

    rdd = usersrdd.map(lambda row: (row['user_id'], row['review_count']))

    default_partition = rdd.sortBy(keyfunc= lambda x: x[1], ascending=False)
    d_n_partition = default_partition.getNumPartitions()
    d_n_items = default_partition.mapPartitions(lambda it: [sum(1 for _ in it)]).collect()

    custom_partition = rdd.sortBy(keyfunc=lambda x: x[1], ascending=False).partitionBy(n_partitions)
    c_n_partition = custom_partition.getNumPartitions()
    c_n_items = custom_partition.mapPartitions(lambda it: [sum(1 for _ in it)]).collect()

    start_time = time.time()
    def_partition = default_partition.take(10)
    end_time = time.time()
    d_exe_time = end_time - start_time

    start_time = time.time()
    cus_partition = custom_partition.take(10)
    end_time = time.time()
    c_exe_time = end_time - start_time

    part1 = {"n_partition": d_n_partition, "n_items": d_n_items, "exe_time": d_exe_time}
    part2 = {"n_partition": c_n_partition, "n_items": c_n_items, "exe_time": c_exe_time}
    partition_task2["default"] = part1
    partition_task2["customized"] = part2
    partition_task2["explanation"] = "The degree of parallelism is determined by the number of RDD partitions. In case of very few partitions, all the cores in the cluster would not be utilized. If there are too many partitions, then too many small tasks get scheduled and there is excessive overhead in managing many small tasks resulting in increased execution time for large datasets.Hence, optimal number of partitions is atleast equal to as many as number of executors for parallelism."

    with open(output_file, 'w+') as outfile:
        json.dump(partition_task2, outfile)








