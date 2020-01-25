// package com.usc.datamining

import java.io.{BufferedWriter, File, FileWriter}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.json4s.{DefaultFormats, JObject}
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization

object swathi_nayak_task1 {

  case class Solution(total_users: Long, avg_reviews: Double, distinct_usernames: Long, num_users: Long, top10_popular_names: List[List[Any]], top10_most_reviews: List[List[Any]])

  def convertJsonToRow(jsonStr: String): Row = {
    implicit val jsonFormat = DefaultFormats

    val key = parse(jsonStr).asInstanceOf[JObject]
    val user_id = (key \ "user_id").extract[String]
    val name = (key \ "name").extract[String]
    val review_count = (key \ "review_count").extract[Long]
    val yelping_since = (key \ "yelping_since").extract[String]
    Row(user_id, name, review_count, yelping_since)
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("swathi_nayak_task1").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    val input_file = args(0)
    val output_file = args(1)
    // val input_file = "/Users/swathinayak/IdeaProjects/HW1/yelp_dataset/user.json"
    val read_file: RDD[String] = sc.textFile(input_file)

    val rdd = read_file.map(x => convertJsonToRow(x))

    val total_users = rdd.count()

    //print(rdd.take(5).toList);
    val total_review_count = rdd.map(r => r.get(2).asInstanceOf[Long]).collect().sum
    val avg_reviews = total_review_count.toDouble / total_users

    val distinct_usernames = rdd.map(r => r.get(1).asInstanceOf[String]).distinct().count()

    val num_users = rdd.filter(r => r.get(3).asInstanceOf[String].contains("2011")).count()

    val top10_popular_names = rdd.map(r => r.get(1).asInstanceOf[String]).map( x => (x,1)).reduceByKey((x,y) => (x+y)).map(x => List(x._1, x._2)).sortBy(-_(1).asInstanceOf[Integer]).take(10)
    val top10_popular_names_2 = top10_popular_names.toList

    val max_reviews = rdd.map(r => List(r.get(0).asInstanceOf[String], r.get(2).asInstanceOf[Long])).sortBy(-_(1).asInstanceOf[Long]).take(10)
    val top10_most_reviews = max_reviews.toList

    val solution = new Solution(total_users, avg_reviews, distinct_usernames, num_users, top10_popular_names_2.toList, top10_most_reviews.toList)
    implicit val formats = org.json4s.DefaultFormats
    // println(Serialization.write(solution))

    // val output_file = "/Users/swathinayak/IdeaProjects/HW1/yelp_dataset/task1_output.json"
    val file = new File(output_file)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(Serialization.write(solution))
    bw.close()

  }

}
