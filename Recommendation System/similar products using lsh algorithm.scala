// package com.datamining.hw3
import java.io._
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer
import scala.collection.{mutable, _}


object swathi_nayak_task1 {

  def generate_permutations(m: Int, n: Int): Map[Long,(Int, Int, Int)] ={
    var rand_values = collection.mutable.Map[Long, (Int, Int, Int)]().withDefaultValue(Tuple3(0,0,0))
    var rand1 = new scala.util.Random
    for( i <- 0 until m){
      var r1 = rand1.nextInt(n + 1)
      var r2 = rand1.nextInt(n + 1)
      rand_values.update(i, new Tuple3(r1, r2, n))
    }
    return rand_values
  }

  def generate_minhash(x: (String, Iterable[String]), user_indices: Map[String, Long], random_permutations: Map[Long, (Int, Int, Int)]): (String, List[Long]) =  {

    var sign_matrix: ListBuffer[Long] = new ListBuffer()
    for(hash_function <- random_permutations)
    {
      var minimum = Long.MaxValue
      for(key <- x._2)
      {
        var row_index = user_indices(key)
        val hash_value = (hash_function._2._1 * row_index + hash_function._2._2) % hash_function._2._3
        if(minimum > hash_value)
        {
          minimum = hash_value
        }
      }
      sign_matrix += minimum
    }
    return Tuple2(x._1, sign_matrix.toList)
  }

  def generate_band(signature: (String, List[Long]), band: Int, row:Int): List[((String, List[Long]), List[String])] = {
    var sign: ListBuffer[((String, List[Long]), List[String])] = new ListBuffer()
    for( i <- 0 until band)
    {
      sign += new Tuple2[(String, List[Long]), List[String]](("part-"+i, signature._2.slice(i*row,(i+1)*row)), List(signature._1))
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

  def main(args : Array[String]): Unit = {

    val start_time = System.currentTimeMillis()
    val conf: SparkConf = new SparkConf().setAppName("swathi_nayak_task1").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    val input_file = args(0)
    val similarity_method = args(1)
    val output_file = args(2)

    if (similarity_method == "jaccard") {

      val raw_data = sc.textFile(input_file)
      var header = raw_data.first()
      var user_prod_data = raw_data.filter(row => row != header).map(row => row.split(",")).map(x => (x(0).toString, x(1).toString))

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

      println(similar_pairs.length)
      val sorted_similar_pairs = similar_pairs.sorted
      val out_file = new PrintWriter(new File(output_file))
      val output_header = "business_id_1, business_id_2, similarity\n"
      out_file.write(output_header)
      for (pair <- sorted_similar_pairs) {
        out_file.write(pair._1 + "," + pair._2 + "," + pair._3 + "\n")
      }
      out_file.close()

      val end_time_2 = System.currentTimeMillis()
      println("Duration: " + (end_time_2 - start_time)/1000 + " seconds")

    }

  }


}
