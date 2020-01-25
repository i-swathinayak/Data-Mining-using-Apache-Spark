from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import binascii
import datetime
import json
import sys


def hash_functions(x):
	m=671
	return [(7*x+1)%m, (3*x+1)%m, (51*x+58)%m, (68*x+73)%m, (17*x+19)%m, (42*x+78)%m, (17*x+19)%m, (136*x+152)%m, (23*x+29)%m, (13*x+19)%m, (11*x+19)%m, (23*x+37)%m]


def implement_flajolet_martin(stream_rdd):

	n = 12
	len_trailing_zero = [0] * n
	max_trailing_zero = [0] * n

	distinct_city = dict()

	data_stream = stream_rdd.collect()

	for business_data in data_stream:

		business = json.loads(business_data)
		bus_city = business['city']
		b_hash = int(binascii.hexlify(bus_city.encode('utf8')), 16)
		mod_vals = [17, 83, 29, 253, 97, 173, 53, 12, 37, 317, 79, 11]
		bus_hash = []
		bus_binary = []

		for i in range(n):
			bus_hash.append(b_hash % mod_vals[i])

		for i in range(n):
			bus_binary.append(format(bus_hash[i], '016b'))

		for i in range(n):
			if len(bus_binary[i]) != len(bus_binary[i])-len(bus_binary[i].rstrip('0')):
				len_trailing_zero[i] = len(bus_binary[i])-len(bus_binary[i].rstrip('0'))

		for i in range(n):
			if len_trailing_zero[i] > max_trailing_zero[i]:
				max_trailing_zero[i] = len_trailing_zero[i]

		distinct_city[bus_city] = 1

	grouped_averages = []
	for i in range(0, n, 2):
		grouped_averages.append((2**max_trailing_zero[i]+2**max_trailing_zero[i+1])//2)
	grouped_averages = sorted(grouped_averages)
	estimate = str(grouped_averages[3])

	f = open(output_file, "a+")
	f.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "," + str(len(distinct_city)) + "," + estimate + "\n")
	f.close()


if __name__ == "__main__":
	port = int(sys.argv[1])
	output_file = sys.argv[2]

	sc = SparkContext()
	sc.setLogLevel(logLevel="ERROR")

	f = open(output_file, "w+")
	f.write("Time,Ground Truth,Estimation\n")
	f.close()

	stream_context = StreamingContext(sc, 5)
	stream_ctxt = stream_context.socketTextStream("localhost", port)
	streaming_window = stream_ctxt.window(30, 10)
	streaming_window.foreachRDD(implement_flajolet_martin)
	stream_context.start()
	stream_context.awaitTermination()
