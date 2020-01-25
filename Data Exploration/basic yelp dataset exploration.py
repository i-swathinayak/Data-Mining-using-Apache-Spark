from pyspark.shell import spark
import operator
import json
import sys
from pyspark import SparkContext

if __name__ == '__main__':
    input_file=sys.argv[1]
    output_file=sys.argv[2]
    explore_data_task_1 = {}

    sc = SparkContext.getOrCreate()
    read_input = sc.textFile(input_file)
    rdd = read_input.map(lambda x: json.loads(x))

    count=rdd.count()
    explore_data_task_1["total_users"]=count

    review_rdd = rdd.map(lambda row: row['review_count'])
    total = review_rdd.sum()
    average = total / count
    explore_data_task_1["avg_reviews"] = average

    distinct_users = rdd.map(lambda row: row['name']).distinct().count()
    explore_data_task_1["distinct_usernames"] = distinct_users

    yelp_users = rdd.filter(lambda row: "2011" in row['yelping_since']).count()
    explore_data_task_1["num_users"] = yelp_users

    popular_names = rdd.keyBy(lambda row: row['name']).countByKey()
    top10_popular_names = list(sorted(popular_names.items(), key=operator.itemgetter(1), reverse=True))[:10]
    explore_data_task_1["top10_popular_names"] = top10_popular_names

    max_reviews = rdd.map(lambda row: (row['user_id'], row['review_count'])).takeOrdered(10, key=lambda x: -x[1])
    explore_data_task_1["top10_most_reviews"] = max_reviews

    with open(output_file, 'w+') as outfile:
        json.dump(explore_data_task_1, outfile)






















