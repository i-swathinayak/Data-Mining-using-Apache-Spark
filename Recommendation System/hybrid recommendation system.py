import time
import json
import sys
from pyspark import SparkContext
import pandas as pd
import nltk
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
import math
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
import csv
from surprise import Dataset
from surprise import Reader
from surprise import BaselineOnly
from pyspark.sql import SparkSession


# model-based
def modelcf_predict_rating(pred_rating):
    if pred_rating > 5.0:
        return 5.0
    elif pred_rating < 0.0:
        return 0.0
    else:
        return pred_rating


# def categoriesForPrinting(categories):
#     cat0 = 0
#     cat1 = 0
#     cat2 = 0
#     cat3 = 0
#     cat4 = 0
#     for category in categories:
#         cat = category[0]
#         if cat == 0:
#             cat0 = category[1]
#         elif cat == 1:
#             cat1 = category[1]
#         elif cat == 2:
#             cat2 = category[1]
#         elif cat == 3:
#             cat3 = category[1]
#         elif cat == 4:
#             cat4 = category[1]
#     return cat0, cat1, cat2, cat3, cat4
#
#
# def category(diff):
#     if(diff>=0 and diff<1):
#         return 0
#     elif(diff>=1 and diff<2):
#         return 1
#     elif(diff>=2 and diff<3):
#         return 2
#     elif(diff>=3 and diff<4):
#         return 3
#     elif(diff>=4):
#         return 4


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

        #if similarity is not None:
        result.append((similarity, user))

    n=len(result)//2
    result = sorted(result, reverse=True)
    return result


def usercf_compute_similarity(pred_user, user):
    pred_user_ratings = users_dict[pred_user]
    user_ratings = users_dict[user]

    puser_products = list(map(set, zip(*pred_user_ratings)))[0]
    cuser_products = list(map(set, zip(*user_ratings)))[0]

    corated_products = list(puser_products.intersection(cuser_products))

    if len(corated_products) == 0 or len(corated_products) == 1:
        return 0.0

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
        return 0.0
    else:
        return numerator/denominator


def usercf_predict_rating(pred_user, pred_item, similar_users):
    pred_user_ratings = users_dict[pred_user]
    pred_user_ratings = [x[1] for x in pred_user_ratings]
    p_avg = sum(pred_user_ratings)/len(pred_user_ratings)
    if len(similar_users) == 0:
        if p_avg > 5.0:
            return 5.0
        elif p_avg<0.0:
            return 0.0
        return p_avg

    else:
        numerator = 0
        denominator = 0
        for similar_user in similar_users:
            if similar_user[0] >= 0.0:
                user = similar_user[1]
                key = (user, pred_item)
                if key in users_products_dict:
                    user_rating = users_products_dict[(user, pred_item)]
                    user_ratings = users_dict[user]
                    user_ratings = [x[1] for x in user_ratings if x[0] != pred_item]
                    user_avg = sum(user_ratings)/len(user_ratings)
                    denominator = denominator + abs(similar_user[0])
                    numerator = numerator + similar_user[0]*(user_rating - user_avg)

        if denominator == 0 or (numerator/denominator < 0):
            user_average = p_avg
        else:
            user_average = p_avg + (numerator/denominator)
        if user_average < 0.0:
            return 0.0
        elif user_average > 5.0:
            return 5.0
        return user_average


def run_naivebayes(user_row):
    # stemmer = PorterStemmer()
    u_row=list()

    for prod_rating in user_row:
        u_row.append(prod_rating)

    # print(u_row)
    panda_df = pd.DataFrame(u_row, columns=['User_id', 'Business_id', 'Features', 'Rating'])
    # panda_df = panda_df.drop('Business_id', 1)
    panda_df['Features'] = panda_df['Features'].apply(', '.join)
    panda_df['Features'] = panda_df['Features'].apply(nltk.word_tokenize)
    # panda_df['Features'] = panda_df['Features'].apply(lambda x: [stemmer.stem(y) for y in x])
    panda_df['Features'] = panda_df['Features'].apply(lambda x: ' '.join(x))
    panda_df['Features'] = panda_df.Features.str.replace('[^\w\s]', '')

    # count_vect = CountVectorizer()
    # counts = count_vect.fit_transform(panda_df['Features'])
    # counts_df = pd.DataFrame(counts, columns=['Features'])
    # new_panda_df = panda_df.assign(Features=counts_df['Features'])

    panda_df_train = panda_df[panda_df.Rating.notnull()]
    count_vect = CountVectorizer()
    counts = count_vect.fit_transform(panda_df_train['Features'])
    panda_df_test = panda_df.loc[panda_df.isnull().any(axis=1)]
    testing_data = list(map(tuple, panda_df_test.values))
    counts_test = count_vect.transform(panda_df_test['Features'])

    # count_vect = CountVectorizer()
    # counts = count_vect.fit_transform(panda_df['Features'])
    # transformer = TfidfTransformer().fit(counts)
    # counts = transformer.transform(counts)
    # X_train, X_test, y_train, y_test = train_test_split(counts, panda_df['Rating'], test_size=0.2, random_state=69)

    y_train = panda_df_train['Rating']
    model = MultinomialNB().fit(counts, y_train)
    predicted = model.predict(counts_test)

    result=list()
    for i in range(len(testing_data)):
        result.append(((testing_data[i][0], testing_data[i][1]), predicted[i]))

    return result


def parse(bus_row):
    business_id = bus_row['business_id']
    categories = bus_row['categories']
    bus_rep = ""
    bus_attributes = ""
    if bus_row['attributes'] is not None:
        for key in bus_row['attributes']:
            if bus_row['attributes'][key] == "True" or bus_row['attributes'][key] == "yes":
                bus_attributes += key + " "
            elif bus_row['attributes'][key] != "False" and bus_row['attributes'][key] != "no" and bus_row['attributes'][key][0] != "{":
                bus_attributes += key + "_" + bus_row['attributes'][key] + " "

    bus_categories = ""
    if categories is not None:
        categories_list = categories.split(",")
        for category in categories_list:
            bus_categories += category.strip() + " "

    bus_rep += bus_attributes + " " + bus_categories
    bus_rep = bus_rep.replace("&", "").lower().strip()
    res = bus_rep.split()
    return (business_id, res)


if __name__ == '__main__':
    start_time = time.time()

    # input parameters
    input_path = sys.argv[1]
    test_file = sys.argv[2]
    result_file = sys.argv[3]

    business_file = input_path + "business.json"
    train_file = input_path + "yelp_train.csv"

    sc = SparkContext.getOrCreate()
    read_input = sc.textFile(business_file)
    business_rdd = read_input.map(lambda x: json.loads(x))
    business_id_data = business_rdd.map(lambda x: parse(x))

    read_input_train = sc.textFile(train_file)
    header_a = read_input_train.first()
    train_rdd = read_input_train.filter(lambda line: line != header_a).map(lambda line: line.split(",")).map(lambda line: (str(line[1]), (str(line[0]), float(line[2]))))
    #
    read_input_test = sc.textFile(test_file)
    header_b = read_input_test.first()
    test_rdd = read_input_test.filter(lambda line: line != header_b).map(lambda line: line.split(",")).map(lambda line: (str(line[1]), (str(line[0]), None)))

    #
    # train_rdd_list, test_rdd_list = train_rdd_1.randomSplit(weights=[0.5, 0.5], seed=1)
    # train_rdd = sc.parallelize(train_rdd_list)
    # test_rdd = sc.parallelize((test_rdd_list))
    test_data = test_rdd.map(lambda x: (x[0], (x[1][0], None)))
    test_rdd_model = test_rdd.map(lambda x: ((x[1][0], x[0]), x[1][1]))
    # pred_rdd = test_rdd.map(lambda x: ((x[1][0], x[0]), x[1][1]))

    # content-based recommendation
    train_test_rdd = train_rdd.union(test_data)
    content_based_ratings = train_test_rdd.join(business_id_data).map(
        lambda x: (x[1][0][0], (x[1][0][0], x[0], x[1][1], x[1][0][1]))).groupByKey().map(lambda x: run_naivebayes(x[1])).flatMap(lambda list: list)




    c_new_predictions = test_rdd_model.subtractByKey(content_based_ratings)
    c_new_predictions = c_new_predictions.map(lambda x: ((x[0][0], x[0][1]), 3.0))
    c_new_predictions = content_based_ratings.union(c_new_predictions)
    # print(new_predictions.count())
    #
    # results = content_based_ratings.join(pred_rdd)
    # differences = results.map(lambda x: abs(x[1][0] - x[1][1]))
    # rmse = math.sqrt(differences.map(lambda x: x ** 2).mean())
    # print(rmse)
    # print(time.time() - start_time)

    # user-based
    train_rdd_2 = read_input_train.filter(lambda line: line != header_a).map(lambda line: line.split(",")).map(
        lambda line: ((str(line[0]), str(line[1])), float(line[2])))
    test_data_2 = test_rdd.map(lambda x: (x[1][0], x[0]))

    users_dict = train_rdd_2.map(lambda x: (x[0][0], (x[0][1], x[1]))).groupByKey().mapValues(list).collectAsMap()
    products_dict = train_rdd_2.map(lambda x: (x[0][1], x[0][0])).groupByKey().mapValues(list).collectAsMap()
    users_products_dict = train_rdd_2.map(lambda x: (x[0], x[1])).collectAsMap()

    similarity_dict = dict()

    similar_users = test_data_2.map(lambda x: (x[0], x[1], usercf_get_similar_users(x[0], x[1])))
    predicted_ratings = similar_users.map(lambda x: ((x[0], x[1]), usercf_predict_rating(x[0], x[1], x[2])))


    # model-based
    train_rdd_model = read_input_train.filter(lambda line: line != header_a).map(lambda line: line.split(",")).map(
        lambda line: ((str(line[0]), str(line[1])), float(line[2])))

    # test_rdd_model = read_input_test.filter(lambda line: line != header_b).map(lambda line: line.split(",")).map(
        # lambda line: ((str(line[0]), str(line[1])), float(line[2])))

    test_rdd_model = test_rdd.map(lambda x: ((x[1][0], x[0]), x[1][1]))
    train_test_rdd_model = train_rdd_model.union(test_rdd_model).reduceByKey(lambda x, y: x)
    users_data = dict()
    products_data = dict()
    rdd_keys = train_test_rdd_model.keys().collect()
    users = [item[0] for item in rdd_keys]
    products = [item[1] for item in rdd_keys]

    for idx, val in enumerate(users):
        users_data[val] = idx

    for idx, val in enumerate(products):
        products_data[val] = idx

    ratings = train_rdd_model.map(lambda l: Rating(users_data.get(l[0][0]), products_data.get(l[0][1]), l[1]))

    users_dict_model = {index: id for id, index in users_data.items()}
    products_dict_model = {index: id for id, index in products_data.items()}

    rank = 10
    numIterations = 20
    lambda_ = 0.3
    model = ALS.train(ratings, rank, numIterations, lambda_)

    testRdd = test_rdd_model.map(lambda x: (users_data.get(x[0][0]), products_data.get(x[0][1])))

    # predictions = model.predictAll(testRdd).map(lambda r: (((users_dict.get(r[0]), products_dict.get(r[1])), r[2])))
    predictions = model.predictAll(testRdd).map(lambda r: ((users_dict_model.get(r[0]), products_dict_model.get(r[1])), r[2]))
    predictions = predictions.map(lambda x: ((x[0][0], x[0][1]), modelcf_predict_rating(x[1])))

    new_predictions = test_rdd_model.subtractByKey(predictions)
    new_predictions = new_predictions.map(lambda x: ((x[0][0], x[0][1]), 3.0))
    new_predictions = predictions.union(new_predictions)
    print(new_predictions.count())


        # ratings_and_preds = test_rdd.join(new_predictions)
        #
        # differences = ratings_and_preds.map(lambda x: abs(x[1][0] - x[1][1])).collect()
        # diff = sc.parallelize(differences)
        # rmse = math.sqrt(diff.map(lambda x: x * x).mean())
        # print(rmse)

    # print(time.time() - start_time)

    # surprise-based
    spark = SparkSession(sc)

    train_rdd_s = read_input_train.filter(lambda x: x != header_a).mapPartitions(lambda x: csv.reader(x)).map(lambda x: (x[0], x[1], float(x[2])))
    test_rdd_s = read_input_test.filter(lambda x: x != header_b).mapPartitions(lambda x: csv.reader(x)).map(lambda x: (x[0], x[1], float(x[2])))
    train_pandas = train_rdd_s.toDF().toPandas()
    train_pandas = train_pandas.rename(columns={'_1': 'user_id', '_2': 'business_id', '_3': 'rating'})
    test_pandas = test_rdd_s.toDF().toPandas()
    test_pandas = test_pandas.rename(columns={'_1': 'user_id', '_2': 'business_id', '_3': 'rating'})
    reader = Reader(rating_scale=(1, 5))
    data = Dataset.load_from_df(train_pandas[['user_id', 'business_id', 'rating']], reader)
    trainset = data.build_full_trainset()
    data_test = Dataset.load_from_df(test_pandas[['user_id', 'business_id', 'rating']], reader)
    bsl_options = {'method': 'als',
                   'n_epochs': 15,
                   'reg_u': 10,
                   'reg_i': 5
                   }
    algo = BaselineOnly(bsl_options=bsl_options)
    algo.fit(trainset)
    predict = list()
    for _, row in test_pandas.iterrows():
        predict.append(algo.predict(row.user_id, row.business_id))
    output_rdd = sc.parallelize(predict)
    output_rdd = output_rdd.map(lambda x: ((x[0], x[1]), float(x[3])))
    print(output_rdd.count())



    # ---------------------------------------------------------------------------------

    # weighted hybrid recommender
    # combined_ratings = content_based_ratings.join(predicted_ratings)
    # combined_ratings_model = combined_ratings.join(new_predictions)
    # combined_ratings_surprise = combined_ratings_model.join(output_rdd)
    # regressor_input = combined_ratings_surprise.join(pred_rdd).map(lambda x: (x[0][0], x[0][1], x[1][0][0][0][0], x[1][0][0][0][1], x[1][0][0][1], x[1][0][1], x[1][1])).collect()

    # linear regression

    # # set weights of recommenders
    w1 = 0.03456651
    w2 = 0.00479681
    w3 = 0.12420057
    w4 = 0.87370496
    bias = -0.11133440740425993

    content_user_ratings = c_new_predictions.join(predicted_ratings)
    combined_ratings = content_user_ratings.join(new_predictions)
    combined_ratings_2 = combined_ratings.join(output_rdd)
    # weighted_predictions = combined_ratings.map(lambda x: (x[0][0], x[0][1], x[1][0] * w1 + x[1][1] * w2 + bias)).collect()
    weighted_predictions = combined_ratings_2.map(lambda x: ((x[0][0], x[0][1]), x[1][0][0][0] * w1 + x[1][0][0][1] * w2 + x[1][0][1] * w3 + x[1][1] * w4 + bias))
    #
    # final_results = weighted_predictions.join(pred_rdd)
    # final_differences = final_results.map(lambda x: abs(x[1][0] - x[1][1]))
    # categories = final_differences.map(lambda x: (category(x), 1)).reduceByKey(lambda x, y: x + y).sortByKey().collect()
    #
    # final_rmse = math.sqrt(final_differences.map(lambda x: x ** 2).mean())
    # print(final_rmse)
    # print(time.time() - start_time)

    out_file = open(result_file, "w")
    output_header = "user_id, business_id, prediction" + "\n"
    out_file.write(output_header)
    for p in weighted_predictions.collect():
        out_file.write(str(p[0][0]) + "," + str(p[0][1]) + "," + str(p[1]) + "\n")
    out_file.close()
    print(time.time() - start_time)

    # # Printing all results
    # cat0, cat1, cat2, cat3, cat4 = categoriesForPrinting(categories)
    # print("Error Distribution")
    # print(">=0 and <1: ", cat0)
    # print(">=1 and <2: ", cat1)
    # print(">=2 and <3: ", cat2)
    # print(">=3 and <4: ", cat3)
    # print(">=4: ", cat4)


