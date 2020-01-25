// package com.usc.datamining

import java.io.{BufferedWriter, File, FileWriter}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.json4s.{DefaultFormats, JObject}
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization
import java.io.{BufferedWriter, File, FileWriter}
import java.io._

object swathi_nayak_task2 {

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
    val n_partitions = args(2).toInt

    // val input_file = "/Users/swathinayak/IdeaProjects/HW1/yelp_dataset/user.json"
    val read_file: RDD[String] = sc.textFile(input_file)
    val userrdd = read_file.map(x => convertJsonToRow(x))

    val rdd = userrdd.map(r => (r.get(0).asInstanceOf[String], r.get(2).asInstanceOf[Long]))

    val default_partition = rdd.sortBy(_._2)
    val d_n_partition = default_partition.getNumPartitions
    val d_n_items = default_partition.glom().map(_.length).collect().toList

    val custom_partition = rdd.sortBy(_._2).partitionBy(new HashPartitioner(n_partitions))
    val c_n_partition = custom_partition.getNumPartitions
    val c_n_items = custom_partition.glom().map(_.length).collect().toList

    val t1 = System.nanoTime
    val def_partition = default_partition.take(10)
    val d_exe_time = (System.nanoTime - t1) / 1e9d

    val t2 = System.nanoTime
    val cus_partition = custom_partition.take(10)
    val c_exe_time = (System.nanoTime - t2) / 1e9d

    val explanation = "The degree of parallelism is determined by the number of RDD partitions. In case of very few partitions, all the cores in the cluster would not be utilized. If there are too many partitions, then too many small tasks get scheduled and there is excessive overhead in managing many small tasks resulting in increased execution time for large datasets.Hence, optimal number of partitions is atleast equal to as many as number of executors for parallelism."

    val defaultResult = Map("n_partition" -> d_n_partition, "n_items" -> d_n_items, "exe_time" -> d_exe_time)
    val customizedResult = Map("n_partition" -> c_n_partition, "n_items" -> c_n_items, "exe_time" -> c_exe_time)

    val task2Result = Map("default" -> defaultResult, "customized" -> customizedResult, "explanation" -> explanation)

    val w: Writer = {
      new FileWriter(output_file)
    }
    implicit val formats = org.json4s.DefaultFormats
    try {
      w.write(Serialization.write(task2Result))
      w.close()
    } catch {
      case e: IOException => e.printStackTrace()
    }


  }

}

