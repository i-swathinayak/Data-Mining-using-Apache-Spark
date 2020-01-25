from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
import time
import random
import math
import sys


# model-based
def modelcf_predict_rating(pred_rating):
    if pred_rating > 5.0:
        return 5.0
    elif pred_rating < 1.0:
        return 1.0
    else:
        return pred_rating


# user-based
def usercf_get_similar_users(pred_user, pred_product):
    result = []

    if pred_user not in users_dict or pred_product not in products_dict:
        return result

    candidate_users = products_dict[pred_product]

    for user in candidate_users:
        #if pred_user != otherUser:
        if (pred_user, user) in similarity_dict:
            similarity = similarity_dict.get((pred_user, user))
        else:
            similarity = usercf_compute_similarity(pred_user, user)
            similarity_dict[(pred_user, user)] = similarity
            similarity_dict[(user, pred_user)] = similarity

        if similarity is not None:
            result.append((similarity, user))

    result = sorted(result, reverse=True)
    return result


def usercf_compute_similarity(pred_user, user):
    pred_user_ratings = users_dict[pred_user]
    user_ratings = users_dict[user]

    puser_products = list(map(set, zip(*pred_user_ratings)))[0]
    cuser_products = list(map(set, zip(*user_ratings)))[0]

    corated_products = list(puser_products.intersection(cuser_products))

    if len(corated_products) == 0 or len(corated_products) == 1:
        return None

    p_ratings = [users_products_dict.get((pred_user, x)) for x in corated_products]
    p_avg = sum(p_ratings)/len(p_ratings)

    c_ratings = [users_products_dict.get((user, x)) for x in corated_products]
    c_avg = sum(c_ratings)/len(c_ratings)

    numerator = 0.0
    p_den = 0.0
    c_den = 0.0
    for i in range(len(corated_products)):
        p = p_ratings[i] - p_avg
        c = c_ratings[i] - c_avg
        numerator = numerator + p*c
        p_den = p_den + (p*p)
        c_den = c_den + (c*c)

    denominator = math.sqrt(p_den) * math.sqrt(c_den)
    if numerator == 0 or denominator == 0:
        return None
    else:
        return numerator/denominator


def usercf_predict_rating(pred_user, pred_item, similar_users):
    pred_user_ratings = users_dict[pred_user]
    pred_user_ratings = [x[1] for x in pred_user_ratings]
    p_avg = sum(pred_user_ratings)/len(pred_user_ratings)

    if len(similar_users) == 0:
        return p_avg

    else:
        numerator = 0
        denominator = 0
        for similar_user in similar_users:
            user = similar_user[1]

            key = (user, pred_item)
            if key in users_products_dict:
                user_rating = users_products_dict[(user, pred_item)]
                user_ratings = users_dict[user]
                user_ratings = [x[1] for x in user_ratings if x[0] != pred_item]
                user_avg = sum(user_ratings)/len(user_ratings)
                denominator = denominator + abs(similar_user[0])
                numerator = numerator + similar_user[0]*(user_rating - user_avg)

        if denominator == 0:
            return p_avg

        rating = (p_avg + numerator/denominator)
        if rating > 5.0:
            return 5.0
        elif rating < 1.0:
            return 1.0
        else:
            return rating

# item-based
'''
def itemcf_get_similar_products(pred_user, pred_product):
    result = []

    if pred_user not in users_dict or pred_product not in products_dict:
        # item never rated by any user (item cold start)
        return result

    pred_user_products = users_dict[pred_user]
    candidate_products = [x[0] for x in pred_user_products]

    for product in candidate_products:
        if (pred_product, product) in similarity_dict_2:
            similarity = similarity_dict_2.get((pred_product, product))
        else:
            similarity = compute_similarity(pred_product, product)
            similarity_dict_2[(pred_product, product)] = similarity
            similarity_dict_2[(product, pred_product)] = similarity

        if similarity is not None:
                result.append((similarity, product))

    result = sorted(result, reverse=True)
    n = len(result)//2
    return result[:n]
'''

def itemcf_compute_similarity(pred_product, product):
    pred_product_users = products_dict[pred_product]
    product_users = products_dict[product]

    corated_users = list(set(pred_product_users).intersection(set(product_users)))

    if len(corated_users) == 0 or len(corated_users) == 1:
        return None

    p_ratings = [users_products_dict.get((x, pred_product)) for x in corated_users]
    p_avg = sum(p_ratings)/len(p_ratings)

    c_ratings = [users_products_dict.get((x, product)) for x in corated_users]
    c_avg = sum(c_ratings)/len(c_ratings)

    numerator = 0.0
    p_den = 0.0
    c_den = 0.0
    for i in range(len(corated_users)):
        p = p_ratings[i] - p_avg
        c = c_ratings[i] - c_avg
        numerator = numerator + p*c
        p_den = p_den + (p*p)
        c_den = c_den + (c*c)

    denominator = math.sqrt(p_den) * math.sqrt(c_den)
    if numerator == 0 or denominator == 0:
        return None
    else:
        return numerator/denominator


def itemcf_predict_rating(pred_user, pred_item, similar_products):
    # average of active user
    pred_user_ratings = users_dict[pred_user]
    pred_user_ratings = [x[1] for x in pred_user_ratings]
    p_avg = sum(pred_user_ratings)/len(pred_user_ratings)

    # similar_products = list(set(similar_products))

    if len(similar_products) == 0:
        return p_avg
        # return 0.0

    else:
        numerator = 0
        denominator = 0
        for similar_product in similar_products:
            product = similar_product[1]

            key = (pred_user, product)
            if key in users_products_dict:
                product_rating = users_products_dict[(pred_user, product)]
                denominator = denominator + abs(similar_product[0])
                numerator = numerator + similar_product[0]*product_rating

        if numerator == 0 or denominator == 0:
            return p_avg
            # return 0.0

        rating = numerator / denominator
        if rating > 5.0:
            return 5.0
        elif rating < 1.0:
            return 1.0
        else:
            return rating


def get_similar_products_lsh(pred_user, pred_product):
    result = []
    if (pred_product not in product1_product2_rdd) and (pred_product not in product2_product1_rdd):
        # no movie similar to active movie
        # assign average rating of user
        return result

    if pred_product not in products_dict:
        # item never rated by any user (item cold start)
        return result

    lsh_similar_products = list()
    if pred_product in product1_product2_rdd:
        lsh_similar_products.extend(product1_product2_rdd[pred_product])
    if pred_product in product2_product1_rdd:
        lsh_similar_products.extend(product2_product1_rdd[pred_product])

    for product in lsh_similar_products:
        if (pred_product, product) in similarity_dict:
            similarity = similarity_dict.get((pred_product, product))
        else:
            similarity = itemcf_compute_similarity(pred_product, product)
            similarity_dict[(pred_product, product)] = similarity
            similarity_dict[(product, pred_product)] = similarity

        if similarity is not None:
                result.append((similarity, product))

    result = sorted(result, reverse=True)
    return result


def compute_matrix_row(user_data, product_dict):
    user_row = [0] * len(product_dict)
    for product in user_data:
        user_row[product_dict.get(product)] = 1
    return user_row


def compute_signature_matrix(characteristic_matrix, users_count, products_count):
    n = 8
    m = users_count

    random_permutations = [[0 for j in range(n)] for i in range(users_count)]
    signature_matrix = [[sys.maxsize for j in range(products_count)] for i in range(n)]

    rand_val = list()
    for c in range(n):
        a = random.randrange(1, m, 2)
        b = random.randint(0, m)
        rand_val.append((a,b))

    for row_index in range(users_count):
        for hash_function in range(n):
            a = rand_val[hash_function][0]
            b = rand_val[hash_function][1]
            random_permutations[row_index][hash_function] = ((a * row_index) + b) % m

        for column in range(products_count):
            if characteristic_matrix[row_index][column] == 1:
                for hash_index in range(n):
                    signature_matrix[hash_index][column] = min(random_permutations[row_index][hash_index],signature_matrix[hash_index][column])

    return signature_matrix


def find_candidate_pairs(band):
    band = list(band)
    product_sign = list(map(list, zip(*band)))
    rows = len(product_sign)
    for i in range(rows):
        for j in range(i + 1, rows):
            if product_sign[i] == product_sign[j]:
                yield(product_dict.get(i), product_dict.get(j))


def compute_jaccard_similarity(product1, product2):
    p1_users = product_users_dict[product1]
    p2_users = product_users_dict[product2]

    corated_users = len(p1_users & p2_users)
    total_users = len(p1_users) + len(p2_users) - corated_users

    return float(corated_users)/float(total_users)


if __name__ == '__main__':
    start_time = time.time()

    train_file = sys.argv[1]
    test_file = sys.argv[2]
    case_id = int(sys.argv[3])
    output_file = sys.argv[4]

    sc = SparkContext.getOrCreate()
    train_data = sc.textFile(train_file)
    header_a = train_data.first()
    train_rdd = train_data.filter(lambda line: line != header_a).map(lambda line: line.split(",")).map(lambda line: ((str(line[0]), str(line[1])), float(line[2])))

    test_data = sc.textFile(test_file)
    header_b = test_data.first()
    test_rdd = test_data.filter(lambda line: line != header_b).map(lambda line: line.split(",")).map(lambda line: ((str(line[0]), str(line[1])), float(line[2])))
    print(test_rdd.count())


    # model-based
    if case_id == 1:
        train_test_rdd = train_rdd.union(test_rdd).reduceByKey(lambda x, y: x)
        users_data = dict()
        products_data = dict()
        rdd_keys = train_test_rdd.keys().collect()
        users = [item[0] for item in rdd_keys]
        products = [item[1] for item in rdd_keys]

        for idx, val in enumerate(users):
            users_data[val] = idx

        for idx, val in enumerate(products):
            products_data[val] = idx

        ratings = train_rdd.map(lambda l: Rating(users_data.get(l[0][0]), products_data.get(l[0][1]), l[1]))

        users_dict = {index: id for id, index in users_data.items()}
        products_dict = {index: id for id, index in products_data.items()}

        rank = 10
        numIterations = 20
        lambda_ = 0.2
        model = ALS.train(ratings, rank, numIterations, lambda_)

        testRdd = test_rdd.map(lambda x: (users_data.get(x[0][0]), products_data.get(x[0][1])))

        # predictions = model.predictAll(testRdd).map(lambda r: (((users_dict.get(r[0]), products_dict.get(r[1])), r[2])))
        predictions = model.predictAll(testRdd).map(lambda r: ((users_dict.get(r[0]), products_dict.get(r[1])), r[2]))
        predictions = predictions.map(lambda x: ((x[0][0], x[0][1]), modelcf_predict_rating(x[1])))

        new_predictions = test_rdd.subtractByKey(predictions)
        new_predictions = new_predictions.map(lambda x: ((x[0][0], x[0][1]), 3.0))
        new_predictions = predictions.union(new_predictions)

        out_file = open(output_file, "w")
        predictions_ = new_predictions.collect()
        print(len(predictions_))
        output_header = "user_id, business_id, prediction" + "\n"
        out_file.write(output_header)
        for prediction in predictions_:
            out_file.write(str(prediction[0][0]) + "," + str(prediction[0][1]) + "," + str(prediction[1]) + "\n")
        out_file.close()

        # ratings_and_preds = test_rdd.join(new_predictions)
        #
        # differences = ratings_and_preds.map(lambda x: abs(x[1][0] - x[1][1])).collect()
        # diff = sc.parallelize(differences)
        # rmse = math.sqrt(diff.map(lambda x: x * x).mean())
        # print(rmse)

        print(time.time() - start_time)

    # user-based
    elif case_id == 2:
        test_data = test_rdd.map(lambda x: (x[0][0], x[0][1]))

        users_dict = train_rdd.map(lambda x: (x[0][0], (x[0][1], x[1]))).groupByKey().mapValues(list).collectAsMap()
        products_dict = train_rdd.map(lambda x: (x[0][1], x[0][0])).groupByKey().mapValues(list).collectAsMap()
        users_products_dict = train_rdd.map(lambda x: (x[0], x[1])).collectAsMap()

        similarity_dict = dict()

        similar_users = test_data.map(lambda x: (x[0], x[1], usercf_get_similar_users(x[0], x[1])))
        predicted_ratings = similar_users.map(lambda x: (x[0], x[1], usercf_predict_rating(x[0], x[1], x[2])))

        predicted_ratings_ = predicted_ratings.collect()
        print(len(predicted_ratings_))
        out_file = open(output_file, "w")
        output_header = "user_id, business_id, prediction" + "\n"
        out_file.write(output_header)
        for p in predicted_ratings_:
            out_file.write(str(p[0]) + "," + str(p[1]) + "," + str(p[2]) + "\n")
        out_file.close()
        print(time.time() - start_time)

        # results = predicted_ratings.map(lambda x: ((x[0], x[1]), x[2])).join(test_rdd)
        # differences = results.map(lambda x: abs(x[1][0] - x[1][1]))
        # rmse = math.sqrt(differences.map(lambda x: x ** 2).mean())
        # print(rmse)
        #
        # print(time.time() - start_time)

    # item-based with LSH
    elif case_id == 3:
        similar_train_rdd = train_rdd.map(lambda x: (x[0][0], x[0][1]))
        test_data = test_rdd.map(lambda x: (x[0][0], x[0][1]))

        users_dict = train_rdd.map(lambda x: (x[0][0], (x[0][1], x[1]))).groupByKey().mapValues(list).collectAsMap()
        products_dict = train_rdd.map(lambda x: (x[0][1], x[0][0])).groupByKey().mapValues(list).collectAsMap()
        users_products_dict = train_rdd.map(lambda x: (x[0], x[1])).collectAsMap()

        bands_count = 8
        users_count = similar_train_rdd.keys().distinct().count()
        product_ids = similar_train_rdd.values().distinct().collect()
        product_ids.sort()
        products_count = len(product_ids)

        product_dict = {}

        for idx, val in enumerate(product_ids):
            product_dict[val] = idx

        prod_data = similar_train_rdd.groupByKey().mapValues(list).values()

        characteristic_matrix = prod_data.map(lambda user_data: compute_matrix_row(user_data, product_dict)).collect()

        signature_matrix = compute_signature_matrix(characteristic_matrix, users_count, products_count)

        signature_matrix_rdd = sc.parallelize(signature_matrix).repartition(bands_count)
        product_dict = {index: id for id, index in product_dict.items()}
        # candidate_pairs = signature_matrix_rdd.mapPartitions(lambda band: find_candidate_pairs(band)).map(lambda x: tuple(sorted(x))).distinct()
        candidate_pairs = signature_matrix_rdd.mapPartitions(lambda band: find_candidate_pairs(band)).distinct()

        product_users_dict = similar_train_rdd.map(lambda line: (str(line[1]), str(line[0]))).groupByKey().mapValues(set).collectAsMap()
        all_similar_pairs = candidate_pairs.map(lambda x: (x, compute_jaccard_similarity(x[0], x[1])))
        similar_pairs = all_similar_pairs.filter(lambda x: x[1] >= 0.5).collect()

        # Predictions with LSH
        product1_product2_rdd = sc.parallelize(similar_pairs).map(lambda x: (x[0][0], x[0][1])).groupByKey().mapValues(
            list).collectAsMap()
        product2_product1_rdd = sc.parallelize(similar_pairs).map(lambda x: (x[0][1], x[0][0])).groupByKey().mapValues(
            list).collectAsMap()

        similarity_dict={}

        similar_products = test_data.map(lambda x: (x[0], x[1], get_similar_products_lsh(x[0], x[1])))
        predicted_ratings = similar_products.map(lambda x: (x[0], x[1], itemcf_predict_rating(x[0], x[1], x[2])))

        predicted_ratings_ = predicted_ratings.collect()
        print(len(predicted_ratings_))
        out_file = open(output_file, "w")
        output_header = "user_id, business_id, prediction" + "\n"
        out_file.write(output_header)
        for p in predicted_ratings_:
            out_file.write(str(p[0]) + "," + str(p[1]) + "," + str(p[2]) + "\n")
        out_file.close()
        # print(time.time() - start_time)
        #
        # results = predicted_ratings.map(lambda x: ((x[0], x[1]), x[2])).join(test_rdd)
        # differences = results.map(lambda x: abs(x[1][0] - x[1][1]))
        # rmse_with_lsh = math.sqrt(differences.map(lambda x: x ** 2).mean())
        # print(rmse_with_lsh)
        #
        # print(time.time() - start_time)









