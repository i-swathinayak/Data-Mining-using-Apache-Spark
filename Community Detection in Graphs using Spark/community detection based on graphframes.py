from pyspark import SparkContext
import time
from graphframes import *
from pyspark import sql
from pyspark.sql.functions import *
import sys


def write_to_file(result_set):
    for community in result_set:
        n = len(community)
        for i in range(n-1):
            file.write("'" + community[i] + "'")
            file.write(", ")
        file.write("'" + community[n-1] + "'")
        file.write("\n")


if __name__ == '__main__':
    start_time = time.time()

    power_input = sys.argv[1]
    output_file = sys.argv[2]
    
    sc = SparkContext.getOrCreate()
    sqlContext = sql.SQLContext(sc)

    train_data = sc.textFile(power_input)
    graph_rdd = train_data.map(lambda line: line.split(" "))
    vertex_rdd = graph_rdd.map(lambda line: (line[0], )).union(graph_rdd.map(lambda line: (line[1], ))).distinct()
    vertex_df = vertex_rdd.toDF(["id"])
    vertex_df.show()

    edge_rdd = graph_rdd.map(lambda line: (line[0], line[1])).union(graph_rdd.map(lambda line: (line[1], line[0]))).distinct()
    edge_df = edge_rdd.toDF(["src", "dst"])
    edge_df.show()

    g = GraphFrame(vertex_df, edge_df)

    result = g.labelPropagation(maxIter=5)
    result_rdd = result.rdd.map(tuple).map(lambda x: (x[1], x[0])).groupByKey().mapValues(list).values().map(lambda x: sorted(x)).collect()
    final_result_set = sorted(result_rdd, key=lambda x: (len(x), x))

    file = open(output_file, "w")
    write_to_file(final_result_set)

    print(time.time() - start_time)








