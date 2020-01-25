// package com.datamining.hw3

import java.io.{File, PrintWriter}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.{Iterable, Map}
import scala.collection.mutable.ListBuffer
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating

object swathi_nayak_task2 {

  // model-based
  def modelcf_predict_rating(pred_rating: Double): Double = {
    if (pred_rating > 5.0) {
      return 5.0
    }
    else {
      if (pred_rating < 1.0) {

        return 1.0
      }
      else {
        return pred_rating
      }
    }
  }

  // user-based
  def usercf_get_similar_users(pred_user: String, pred_product: String, users_dict: Map[String, Iterable[(String, Float)]], products_dict:
  Map[String, Iterable[String]], users_products_dict: Map[(String, String), Float]):
  List[Tuple2[Double, String]] = {

    //println("Entered: get_similar_items")

    var result: ListBuffer[Tuple2[Double, String]] = ListBuffer()

    if (!users_dict.contains(pred_user) || !products_dict.contains(pred_product)) {
      return result.toList
    }

    val candidate_users = products_dict(pred_product)

    var similarity_dict = collection.mutable.Map[Tuple2[String, String], Double]().withDefaultValue(0)

    var similarity: Double = 0.0
    for (user <- candidate_users) {
      if (similarity_dict.contains((pred_user, user))) {
        similarity = similarity_dict((pred_user, user))
      }
      else {
        similarity = usercf_compute_similarity(pred_user, user, users_dict, products_dict, users_products_dict)
        similarity_dict.update((pred_user, user), similarity)
        similarity_dict.update((user, pred_user), similarity)
      }

      if (similarity != -100.0) {
        result += ((similarity, user))
      }

    }

    val n = result.size / 2
    return result.sorted.reverse.take(n).toList

  }

  def usercf_compute_similarity(pred_user: String, user: String, users_dict: Map[String, Iterable[(String, Float)]], products_dict:
  Map[String, Iterable[String]], users_products_dict: Map[(String, String), Float]): Double = {

    //println("Entered: compute_similarity")
    var pred_user_ratings = users_dict(pred_user)
    var user_ratings = users_dict(user)

    val puser_products = pred_user_ratings.map(_._1).toSet
    val cuser_products = user_ratings.map(_._1).toSet

    var corated_products = (puser_products & cuser_products).toList

    if (corated_products.size == 0 || corated_products.size == 1) {
      return -100.0
    }

    val p_ratings: ListBuffer[Double] = ListBuffer()
    val c_ratings: ListBuffer[Double] = ListBuffer()

    for (x <- corated_products) {
      p_ratings += users_products_dict((pred_user, x))
    }

    for (x <- corated_products) {
      c_ratings += users_products_dict((user, x))
    }

    val p_avg = p_ratings.sum / p_ratings.size
    val c_avg = c_ratings.sum / c_ratings.size

    var numerator = 0.0
    var p_den = 0.0
    var c_den = 0.0

    for (i <- 0 to (corated_products.size - 1)) {
      var p = p_ratings(i) - p_avg
      var c = c_ratings(i) - c_avg
      numerator = numerator + p * c
      p_den = p_den + (p * p)
      c_den = c_den + (c * c)
    }

    val denominator = math.sqrt(p_den) * math.sqrt(c_den)
    if (numerator == 0 || denominator == 0) {
      return -100.0
    }
    else {
      return numerator / denominator
    }
  }


  def usercf_predict_rating(pred_user: String, pred_product: String, similar_users: List[Tuple2[Double, String]], users_dict: Map[String, Iterable[(String, Float)]], products_dict:
  Map[String, Iterable[String]], users_products_dict: Map[(String, String), Float]): Double = {

    // println("Entered: predict_rating")
    var pred_user_ratings = users_dict(pred_user)
    val p_ratings: ListBuffer[Double] = ListBuffer()

    for (x <- pred_user_ratings) {
      p_ratings += x._2
    }

    var p_avg = p_ratings.sum / pred_user_ratings.size

    if (similar_users.size == 0) {
      return p_avg
    }

    else {
      var numerator = 0.0
      var denominator = 0.0
      for (similar_user <- similar_users) {
        var user = similar_user._2
        var key = (user, pred_product)
        if (users_products_dict.contains(key)) {
          var user_rating = users_products_dict((user, pred_product))
          var user_ratings = users_dict(user)
          val u_ratings: ListBuffer[Float] = ListBuffer()
          for (x <- user_ratings) {
            if (x._1 != pred_product) {
              u_ratings += x._2
            }
          }
          var user_avg = u_ratings.sum / u_ratings.size
          denominator = denominator + math.abs(similar_user._1)
          numerator = numerator + similar_user._1 * (user_rating - user_avg)
        }
      }

      if (denominator == 0) {
        return p_avg
      }
      val rating = p_avg + numerator / denominator
      if (rating > 5.0) {
        return 5.0
      }
      else {
        if (rating < 1.0) {
          return 1.0
        }
        else {
          return rating
        }
      }

    }


  }

  // item-based with LSH
  def generate_permutations(m: Int, n: Int): Map[Long, (Int, Int, Int)] = {
    var rand_values = collection.mutable.Map[Long, (Int, Int, Int)]().withDefaultValue(Tuple3(0, 0, 0))
    var rand1 = new scala.util.Random
    for (i <- 0 until m) {
      var r1 = rand1.nextInt(n + 1)
      var r2 = rand1.nextInt(n + 1)
      rand_values.update(i, new Tuple3(r1, r2, n))
    }
    return rand_values
  }

  def generate_minhash(x: (String, Iterable[String]), user_indices: Map[String, Long], random_permutations: Map[Long, (Int, Int, Int)]): (String, List[Long]) = {

    var sign_matrix: ListBuffer[Long] = new ListBuffer()
    for (hash_function <- random_permutations) {
      var minimum = Long.MaxValue
      for (key <- x._2) {
        var row_index = user_indices(key)
        val hash_value = (hash_function._2._1 * row_index + hash_function._2._2) % hash_function._2._3
        if (minimum > hash_value) {
          minimum = hash_value
        }
      }
      sign_matrix += minimum
    }
    return Tuple2(x._1, sign_matrix.toList)
  }

  def generate_band(signature: (String, List[Long]), band: Int, row: Int): List[((String, List[Long]), List[String])] = {
    var sign: ListBuffer[((String, List[Long]), List[String])] = new ListBuffer()
    for (i <- 0 until band) {
      sign += new Tuple2[(String, List[Long]), List[String]](("part-" + i, signature._2.slice(i * row, (i + 1) * row)), List(signature._1))
    }
    return sign.toList

  }

  def compute_jaccard_similarity(pair: List[String], candidates_dict: Map[String, Iterable[String]]): Tuple3[String, String, Double] = {

    var p1_users = candidates_dict(pair(0)).toSet
    var p2_users = candidates_dict(pair(1)).toSet
    val corated_users = p1_users.intersect(p2_users).size
    val total_users = p1_users.size + p2_users.size - corated_users
    val j_similarity = corated_users.toDouble / total_users
    val sorted_pair = pair.sorted
    return new Tuple3[String, String, Double](sorted_pair(0), sorted_pair(1), j_similarity)

  }

  def itemcf_get_similar_products(pred_user: String, pred_product: String, products_dict: Map[String, Iterable[String]], users_dict: Map[String, Iterable[(String, Float)]], users_products_dict: Map[(String, String), Float], product1_product2_rdd: Map[String, Iterable[String]], product2_product1_rdd: Map[String, Iterable[String]]):
  List[Tuple2[Double, String]] = {

    //println("Entered: get_similar_items")

    var result: ListBuffer[Tuple2[Double, String]] = ListBuffer()
    // var result = List[Tuple2[Float, String]]

    if (!product1_product2_rdd.contains(pred_product) || !product2_product1_rdd.contains(pred_product) || !products_dict.contains(pred_product)) {
      return result.toList
    }

    var lsh_similar_products: ListBuffer[String] = ListBuffer()

    if (product1_product2_rdd.contains(pred_product)) {
      var temp = product1_product2_rdd(pred_product)
      lsh_similar_products ++= temp
    }


    if (product2_product1_rdd.contains(pred_product)) {
      var temp = product2_product1_rdd(pred_product)
      lsh_similar_products ++= temp
    }

    var similarity_dict = collection.mutable.Map[Tuple2[String, String], Double]().withDefaultValue(0)

    var similarity: Double = 0.0
    for (product <- lsh_similar_products) {
      if (similarity_dict.contains((pred_product, product))) {
        similarity = similarity_dict((pred_product, product))
      }
      else {
        similarity = itemcf_compute_similarity(pred_product, product, users_dict, products_dict, users_products_dict)
        similarity_dict.update((pred_product, product), similarity)
        similarity_dict.update((product, pred_product), similarity)

        if (similarity != -100.0) {
          result += ((similarity, product))
        }
      }
    }

    val n = result.size / 2
    return result.sorted.reverse.take(n).toList


  }

  def itemcf_compute_similarity(pred_product: String, product: String, users_dict: Map[String, Iterable[(String, Float)]], products_dict:
  Map[String, Iterable[String]], users_products_dict: Map[(String, String), Float]): Double = {

    //println("Entered: compute_similarity")
    var pred_product_users = products_dict(pred_product).toSet
    var product_users = products_dict(product).toSet

    var corated_users = (pred_product_users & product_users).toList

    if (corated_users.size == 0 || corated_users.size == 1) {
      return -100.0
    }

    val p_ratings: ListBuffer[Double] = ListBuffer()
    val c_ratings: ListBuffer[Double] = ListBuffer()

    for (x <- corated_users) {
      p_ratings += users_products_dict((x, pred_product))
    }

    for (x <- corated_users) {
      c_ratings += users_products_dict((x, product))
    }

    val p_avg = p_ratings.sum / p_ratings.size
    val c_avg = c_ratings.sum / c_ratings.size

    var numerator = 0.0
    var p_den = 0.0
    var c_den = 0.0

    for (i <- 0 to (corated_users.size - 1)) {
      var p = p_ratings(i) - p_avg
      var c = c_ratings(i) - c_avg
      numerator = numerator + p * c
      p_den = p_den + (p * p)
      c_den = c_den + (c * c)
    }

    val denominator = math.sqrt(p_den) * math.sqrt(c_den)
    if (numerator == 0 || denominator == 0) {
      return -100.0
    }
    else {
      return numerator / denominator
    }
  }


  def itemcf_predict_rating(pred_user: String, pred_product: String, similar_products: List[Tuple2[Double, String]], users_dict: Map[String, Iterable[(String, Float)]], products_dict:
  Map[String, Iterable[String]], users_products_dict: Map[(String, String), Float]): Double = {

    // println("Entered: predict_rating")
    var pred_user_ratings = users_dict(pred_user)
    val p_ratings: ListBuffer[Double] = ListBuffer()

    for (x <- pred_user_ratings) {
      p_ratings += x._2
    }

    var p_avg = p_ratings.sum / pred_user_ratings.size

    if (similar_products.size == 0) {
      return p_avg
    }

    else {
      var numerator = 0.0
      var denominator = 0.0
      for (similar_product <- similar_products) {
        var product = similar_product._2
        var key = (pred_user, product)
        if (users_products_dict.contains(key)) {
          var product_rating = users_products_dict((pred_user, product))
          denominator = denominator + math.abs(similar_product._1)
          numerator = numerator + similar_product._1 * product_rating
        }
      }

      if (numerator == 0 || denominator == 0) {
        return p_avg
      }
      val rating = numerator / denominator
      if (rating > 5.0) {
        return 5.0
      }
      else {
        if (rating < 1.0) {
          return 1.0
        }
        else {
          return rating
        }
      }

    }


  }

  // main

  def main(args: Array[String]): Unit = {

    val start_time = System.currentTimeMillis()
    val conf: SparkConf = new SparkConf().setAppName("swathi_nayak_task2").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    val train_file = args(0)
    val test_file = args(1)
    val case_id = args(2).toInt
    val output_file = args(3)

    // val train_file = "/Users/swathinayak/IdeaProjects/DataMining-HW3/src/main/scala/com/datamining/hw3/yelp_train.csv"
    // val test_file = "/Users/swathinayak/IdeaProjects/DataMining-HW3/src/main/scala/com/datamining/hw3/yelp_val.csv"
    // val output_file = "//Users/swathinayak/IdeaProjects/DataMining-HW3/src/main/scala/com/datamining/hw3/userbasedcf_output.txt"

    var train_data = sc.textFile(train_file)
    var header_a = train_data.first()
    var train_rdd = train_data.filter(row => row != header_a).map(row => row.split(",")).map(x => ((x(0).toString, x(1).toString), x(2).toFloat))

    var test_data = sc.textFile(test_file)
    var header_b = test_data.first()
    var test_rdd = test_data.filter(row => row != header_a).map(row => row.split(",")).map(x => ((x(0).toString, x(1).toString), x(2).toFloat))

    var user_prod_data = train_data.filter(row => row != header_a).map(row => row.split(",")).map(x => (x(0).toString, x(1).toString))

    if (case_id == 1)
      {
        val train_test_rdd = train_rdd.union(test_rdd).reduceByKey((x,y) => x)

        var users_data = collection.mutable.Map[String, Int]().withDefaultValue(0)
        var products_data = collection.mutable.Map[String, Int]().withDefaultValue(0)
        var users_dict = collection.mutable.Map[Int, String]()
        var products_dict = collection.mutable.Map[Int, String]()

        val users = train_test_rdd.keys.map(_._1).collect()
        val products = train_test_rdd.keys.map(_._2).collect()

        for ((elem, i) <- users.zipWithIndex) {
          users_data.update(elem, i)
        }

        for ((elem, i) <- products.zipWithIndex) {
          products_data.update(elem, i)
        }

        for ((k,v) <- users_data){
          users_dict.update(v,k)
        }

        for ((k,v) <- products_data){
          products_dict.update(v,k)
        }

        val ratings = train_rdd.map(l => Rating(users_data(l._1._1), products_data(l._1._2), l._2.toDouble))

        val rank = 10
        val numIterations = 20
        val lambda_ = 0.2
        val model = ALS.train(ratings, rank, numIterations, lambda_)

        val testRdd = test_rdd.map( x => (users_data(x._1._1), products_data(x._1._2)))

        val predictions = model.predict(testRdd).map { case Rating(user, product, rate) => ((user, product), rate) }
        val map_predictions = predictions.map(l => ((users_dict(l._1._1), products_dict(l._1._2)), l._2))

        val new_predictions = map_predictions.map(x => ((x._1._1, x._1._2), modelcf_predict_rating(x._2)))

        val cold_predictions = test_rdd.subtractByKey(new_predictions)
        val gen_predictions = cold_predictions.map( x =>  ((x._1._1, x._1._2), 3.0))
        val predictions_ = gen_predictions.union(new_predictions)

        val out_file = new PrintWriter(new File(output_file))
        val output_header = "user_id, business_id, prediction\n"
        out_file.write(output_header)
        for (pair <- predictions_.collect()) {
          out_file.write(pair._1._1 + "," + pair._1._2 + "," + pair._2 + "\n")
        }
        out_file.close()
 //       val end_time = System.currentTimeMillis()
 //       println("Duration: " + (end_time - start_time)/1000 + " seconds")

//        val rates_preds = test_rdd.join(predictions_)
//        println(rates_preds.count())
//        val MSE = math.sqrt(rates_preds.map { case ((user, product), (r1, r2)) => val err = r1 - r2
//          err * err
//        }.mean())
//        println(s"Mean Squared Error = $MSE")

      }

      if (case_id == 2){

        var new_test_data = test_rdd.map( x =>  (x._1._1, x._1._2))

        var users_dict = train_rdd.map(x =>  (x._1._1, (x._1._2, x._2))).groupByKey().collectAsMap()
        var products_dict = train_rdd.map( x => (x._1._2, x._1._1)).groupByKey().collectAsMap()
        val users_products_dict = train_rdd.map( x =>  (x._1, x._2)).collectAsMap()

        var similar_users = new_test_data.map(x => (x._1, x._2, usercf_get_similar_users(x._1, x._2, users_dict, products_dict, users_products_dict)))
        var predicted_ratings = similar_users.map( x =>  (x._1, x._2, usercf_predict_rating(x._1, x._2, x._3, users_dict, products_dict, users_products_dict)))

        var final_output = predicted_ratings.collect()

        val out_file = new PrintWriter(new File(output_file))
        val output_header = "user_id, business_id, prediction\n"
        out_file.write(output_header)
        for (pair <- final_output) {
          out_file.write(pair._1 + "," + pair._2 + "," + pair._3 + "\n")
        }
        out_file.close()

//        val end_time = System.currentTimeMillis()
//        println("Duration: " + (end_time - start_time)/1000 + " seconds")
//
//        var results = predicted_ratings.map( x => ((x._1, x._2), x._3)).join(test_rdd)
//
//        var differences = results.map( x => math.abs(x._2._1 - x._2._2))
//        var rmse = math.sqrt(differences.map( x => math.pow(x,2)).mean())
//        println(rmse)

        // val end_time_2 = System.currentTimeMillis()
        // println("Duration: " + (end_time_2 - start_time)/1000 + " seconds")
      }

    if (case_id == 3){

      val users_data = user_prod_data.keys.distinct().collect()
      val products_data = user_prod_data.values.distinct().collect()

      var user_indices = collection.mutable.Map[String, Long]().withDefaultValue(0)
      var product_indices = collection.mutable.Map[String, Long]().withDefaultValue(0)

      for (i <- 0 until users_data.length) {
        user_indices.update(users_data(i), i)
      }

      for (i <- 0 until products_data.length) {
        product_indices.update(products_data(i), i)
      }

      // set n and b
      var n = 28
      var b = 14

      val candidates = user_prod_data.map(x => (x._2, x._1)).groupByKey()
      val candidates_dict = candidates.collectAsMap()

      var random_permutations = generate_permutations(n, users_data.length)
      val signature_matrix = candidates.map(x => generate_minhash(x, user_indices, random_permutations))

      val candidate_product_pairs = signature_matrix.flatMap(x => generate_band(x, b, 2)).reduceByKey((x, y) => x ++ y.distinct).flatMap(x => x._2.combinations(2).toList).distinct()

      val similar_pairs = candidate_product_pairs.map(x => compute_jaccard_similarity(x, candidates_dict)).filter(x => x._3 >= 0.5).collect()

      // Prediction with LSH
      val product1_product2_rdd = sc.parallelize(similar_pairs).map( x => (x._1, x._2)).groupByKey().collectAsMap()
      val product2_product1_rdd = sc.parallelize(similar_pairs).map( x =>  (x._2, x._1)).groupByKey().collectAsMap()

      var new_test_data = test_rdd.map( x =>  (x._1._1, x._1._2))
      var users_dict = train_rdd.map(x =>  (x._1._1, (x._1._2, x._2))).groupByKey().collectAsMap()
      var products_dict = train_rdd.map( x => (x._1._2, x._1._1)).groupByKey().collectAsMap()
      val users_products_dict = train_rdd.map( x =>  (x._1, x._2)).collectAsMap()

      var similar_products = new_test_data.map(x => (x._1, x._2, itemcf_get_similar_products(x._1, x._2, products_dict, users_dict, users_products_dict, product1_product2_rdd, product2_product1_rdd)))
      var predicted_ratings = similar_products.map( x =>  (x._1, x._2, itemcf_predict_rating(x._1, x._2, x._3, users_dict, products_dict, users_products_dict)))

      var final_output = predicted_ratings.collect()
      // println(final_output.size)
      // println(test_rdd.count())
      val out_file = new PrintWriter(new File(output_file))
      val output_header = "user_id, business_id, prediction\n"
      out_file.write(output_header)
      for (pair <- final_output) {
        out_file.write(pair._1 + "," + pair._2 + "," + pair._3 + "\n")
      }
      out_file.close()

//      val end_time = System.currentTimeMillis()
//      println("Duration: " + (end_time - start_time)/1000 + " seconds")
//
//      var results = predicted_ratings.map( x => ((x._1, x._2), x._3)).join(test_rdd)
//      println(results.count())
//
//      var differences = results.map( x => math.abs(x._2._1 - x._2._2))
//      var rmse = math.sqrt(differences.map( x => math.pow(x,2)).mean())
//      println(rmse)



    }



  }
}

