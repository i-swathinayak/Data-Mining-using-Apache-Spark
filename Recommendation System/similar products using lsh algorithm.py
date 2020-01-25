from pyspark import SparkContext
import time
import sys
import random
from itertools import combinations


def generate_permutations(m, n):

    rand_values = {}

    for i in range(m):
        a = random.randrange(1, n, 2)
        b = random.randint(0, n)
        rand_values[i] = (a, b, n)

    return rand_values


def generate_minhash(x, user_indices, random_permutations):

    sign_matrix = list()
    for key, hash_function in random_permutations.items():
        minimum = sys.maxsize
        for user in x[1]:
            row_index = user_indices[user]
            hash_value = (hash_function[0] * row_index + hash_function[1]) % hash_function[2]
            if (minimum > hash_value):
                minimum = hash_value
        sign_matrix.append(minimum)
    return (x[0], sign_matrix)


def compute_jaccard_similarity(pair, candidates_dict):

    p1_users = set(candidates_dict[pair[0]])
    p2_users = set(candidates_dict[pair[1]])
    corated_users = len(p1_users & p2_users)
    total_users = len(p1_users) + len(p2_users) - corated_users
    j_similarity = corated_users / total_users
    sorted_pair = sorted(pair)
    return (sorted_pair[0], sorted_pair[1], j_similarity)


def generate_band(signature, band, row):
    sign = list()
    for i in range(0, band):
        sign.append((("part-"+str(i), tuple(list(signature[1])[i*row : (i+1)*row])), [signature[0]]))
    return sign


if __name__ == '__main__':
    start_time = time.time()

    input_file = sys.argv[1]
    similarity_method = sys.argv[2]
    output_file = sys.argv[3]
    # bands_count = 8

    sc = SparkContext.getOrCreate()
    raw_data = sc.textFile(input_file)

    header = raw_data.first()
    user_prod_data = raw_data.filter(lambda line: line != header ).map(lambda line: line.split(",")).map(lambda line: (str(line[0]), str(line[1])))

    if similarity_method == "jaccard":
        # users_data = user_prod_data.keys.distinct().collect()
        users_data = user_prod_data.keys().distinct().collect()
        products_data = user_prod_data.values().distinct().collect()

        user_indices = {}
        product_indices = {}

        for idx, val in enumerate(users_data):
            user_indices[val] = idx

        for idx, val in enumerate(products_data):
            product_indices[val] = idx

        # set n and b
        n = 26
        b = 13

        candidates = user_prod_data.map(lambda x:  (x[1], x[0])).groupByKey()
        candidates_dict = candidates.collectAsMap()

        random_permutations = generate_permutations(n, len(users_data))
        signature_matrix = candidates.map(lambda x: generate_minhash(x, user_indices, random_permutations))

        candidate_product_pairs = signature_matrix.flatMap(lambda x: generate_band(x, b, 2)).reduceByKey(lambda x, y: x+y).map(lambda x: (x[0], set(x[1]))).flatMap(lambda x: combinations(x[1], 2)).distinct()

        similar_pairs = candidate_product_pairs.map(lambda x: compute_jaccard_similarity(x, candidates_dict)).filter(lambda x: x[2] >= 0.5).distinct().collect()

        # output to file
        out_file = open(output_file, "w")
        output_header = "business_id_1, business_id_2, similarity" + "\n"
        out_file.write(output_header)
        sorted_result = sorted(similar_pairs)
        # print(len(sorted_result))
        for i in range(0, len(sorted_result)):
            data = str(sorted_result[i][0]) + "," + str(sorted_result[i][1]) + "," + str(sorted_result[i][2]) + "\n"
            out_file.write(data)

        end = time.time()
        print("Time: ", end - start_time, " seconds")

    elif similarity_method == "cosine":
        print("Cosine not implemented")
    else:
        print("Please provide a valid option: jaccard or cosine")


