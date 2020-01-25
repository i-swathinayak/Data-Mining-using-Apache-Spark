// package com.usc.datamining

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD._
import org.json4s.jackson.Serialization
import java.io._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.json4s.{DefaultFormats, JObject}
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization
import org.apache.spark.{SparkConf, SparkContext}

object swathi_nayak_task3 {

  case class Solution(m1: Double, m2: Double, explanation: String)

  def businessconverttorow(jsonStr: String): Row = {
    implicit val jsonFormat = DefaultFormats
    val key = parse(jsonStr).asInstanceOf[JObject]
    val business_id = (key \ "business_id").extract[String]
    val state = (key \ "state").extract[String]
    Row(business_id, state)

  }

  def reviewconverttorow(jsonStr: String): Row = {

    implicit val jsonFormat = DefaultFormats
    val key = parse(jsonStr).asInstanceOf[JObject]
    val business_id = (key \ "business_id").extract[String]
    val stars = (key \ "stars").extract[Double]
    Row(business_id, stars)

  }

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("swathi_nayak_task3").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    val input_file_1 = args(1)
    val input_file_2 = args(0)
    val output_file_1 = args(2)
    val output_file_2 = args(3)

    // val input_file_1 = "/Users/swathinayak/IdeaProjects/HW1/yelp_dataset/business.json"
    // val input_file_2 = "/Users/swathinayak/IdeaProjects/HW1/yelp_dataset/review.json"
    // val output_file_1= "/Users/swathinayak/IdeaProjects/HW1/yelp_dataset/task3a_output.json"
    // val output_file_2 = "/Users/swathinayak/IdeaProjects/HW1/yelp_dataset/task3b_output.json"

    val read_file_1: RDD[String] = sc.textFile(input_file_1)
    val rdd1 = read_file_1.map(x => businessconverttorow(x))
    val read_file_2:RDD[String] = sc.textFile(input_file_2)
    val rdd2 = read_file_2.map(x => reviewconverttorow(x))

    val business_rdd = rdd1.map(r => (r.get(0).asInstanceOf[String], r.get(1).asInstanceOf[String]))
    val review_rdd = rdd2.map(r => (r.get(0).asInstanceOf[String], r.get(1).asInstanceOf[Double]))
    val stars_rdd = business_rdd.join(review_rdd).map(r => (r._2._1, r._2._2))

    val initial = (0.0d, 0)
    val total_stars_rdd = stars_rdd.aggregateByKey(initial)(
      { case ((total, count), a ) => (total + a, count + 1) },
      { case ((total1, count1), (total2, count2)) => (total1 + total2, count1 + count2) }
    )
    val avg_stars_rdd = total_stars_rdd.map{ case(key,(total, count)) => (key, total / count) }.sortBy(r => (-r._2, r._1)).collect()
    // println(avg_stars_rdd)

    // write to file

    val w1: Writer = {
      new FileWriter(output_file_1)
    }
    try {
      w1.write("state,stars")
      avg_stars_rdd.foreach(x => w1.write("\n"+ x._1 + "," + x._2))
      w1.close()
    } catch {
      case e: IOException => e.printStackTrace()
    }


    // method 1
    val start_time_c = System.nanoTime
    val top_stars_1 = total_stars_rdd.map{ case(key,(total, count)) => (key, total / count) }.sortBy(r => (-r._2, r._1)).map(_._1).collect().toList.take(5)
    println(top_stars_1)
    val collect_diff = (System.nanoTime - start_time_c) / 1e9d
    // println(collect_diff.getClass)

    // method 2
    val start_time_t = System.nanoTime
    val top_stars_2 = total_stars_rdd.map{ case(key,(total, count)) => (key, total / count) }.sortBy(r => (-r._2, r._1)).map(_._1).take(5).toList
    println(top_stars_2)
    val take_diff = (System.nanoTime - start_time_t) / 1e9d

    val time_diff = collect_diff - take_diff

    val explanation = "When a collect operation is issued on a RDD, the entire dataset is copied to the the master node whereas take action retrieves only the specified number of elements. Hence, as can be seen from the example above, the execution time of 'collect' action is higher than that of 'take' action."

    val solution = new Solution(collect_diff, take_diff, explanation)
    implicit val formats = org.json4s.DefaultFormats
    // println(Serialization.write(solution))

    val file = new File(output_file_2)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(Serialization.write(solution))
    bw.close()


  }

}
