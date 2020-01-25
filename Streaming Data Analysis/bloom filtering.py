from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import binascii
import datetime
import json
import sys


def hash_functions(x):
    a = (533*x + 131) % n
    b = (373*x + 87) % n
    return a, b


def implement_bloom_filtering(stream_rdd):

    global bloom_array, state_set, false_positives, true_negatives
    data_stream = stream_rdd.collect()

    for business_data in data_stream:
        business = json.loads(business_data)
        bus_state = business['state']

        bus_int = int(binascii.hexlify(bus_state.encode('utf8')), 16)

        bloom_vals = []

        hashed_state = hash_functions(bus_int)
        for i in hashed_state:
            bloom_vals.append(bloom_array[i])

        if sum(bloom_vals) == 2:
            if bus_state not in state_set:
                false_positives += 1
            state_set.add(bus_state)
        else:
            for i in hashed_state:
                bloom_array[i] = 1
            state_set.add(bus_state)
            true_negatives += 1

    den = false_positives + true_negatives
    if den != 0:
        fpr = false_positives / den
        f = open(output_file, "a+")
        f.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "," + str(fpr) + "\n")
        f.close()


if __name__ == "__main__":
    port = int(sys.argv[1])
    output_file = sys.argv[2]

    sc = SparkContext()
    sc.setLogLevel(logLevel="ERROR")

    f = open(output_file, "w+")
    f.write("Time,FPR\n")
    f.close()

    n = 200
    bloom_array = [0] * n
    state_set = set()
    false_positives = 0
    true_negatives = 0

    stream_context = StreamingContext(sc, 10)
    stream_ctxt = stream_context.socketTextStream("localhost", port)
    stream_ctxt.foreachRDD(implement_bloom_filtering)
    stream_context.start()
    stream_context.awaitTermination()


