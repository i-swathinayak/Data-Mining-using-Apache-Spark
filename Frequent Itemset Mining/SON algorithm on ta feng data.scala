// package com.usc.datamining

import java.io._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import util.control.Breaks
import java.nio.file.Paths


object swathi_nayak_task2 {


  def global_frequent_items(basket: Iterator[Set[Long]], broadcasted: Broadcast[Array[List[Long]]]): Iterator[(List[Long], Int)] = {
    var local_baskets = basket.toList
    var items_counter = collection.mutable.Map[List[Long], Int]().withDefaultValue(0)

    for (candidate <- broadcasted.value) {
      val scandidate = candidate.toSet
      val tcandidate = candidate.sorted
      for (basket <- local_baskets) {
        if (scandidate.subsetOf(basket)) {
          items_counter.update(tcandidate, items_counter(tcandidate) + 1)
        }
      }
    }

    return items_counter.iterator

  }


  def local_frequent_items(basket: Iterator[Set[Long]], threshold: Int, total_baskets: Int): Iterator[List[Long]] = {
    var local_baskets = basket.toList
    val partition_support = threshold * (local_baskets.length / total_baskets)

    var partition_result: ListBuffer[List[Long]] = ListBuffer()
    var items_counter = collection.mutable.Map[Long, Int]().withDefaultValue(0)

    for (basket <- local_baskets) {
      for (item <- basket) {
        items_counter.update(item, items_counter(item) + 1)
      }
    }

    var true_freq_items = new ListBuffer[Long]()
    for ((k, v) <- items_counter) {
      if (v >= partition_support) {
        true_freq_items += k
      }

    }
    val true_freq_items_list = true_freq_items.toList.sorted
    val filter_freq_items_1: ListBuffer[List[Long]] = ListBuffer()

    for (item <- true_freq_items_list) {
      filter_freq_items_1 += List(item)
      partition_result += List(item)
    }

    var size = 2

    val mybreaks = new Breaks
    import mybreaks.{break, breakable}

    var filter_freq_items_lis: List[List[Long]] = List()
    filter_freq_items_lis = filter_freq_items_1.toList

    while (filter_freq_items_lis.length > 0) {
      // println(size)
      val count = filter_freq_items_lis.length
      var candidate_items: ListBuffer[List[Long]] = ListBuffer()
      var true_freq_items: ListBuffer[List[Long]] = ListBuffer()
      val items_counter = collection.mutable.Map[List[Long], Int]().withDefaultValue(0)

      if (size == 2) {
        val res = filter_freq_items_lis.flatten.toSet
        val pairs = res.toSeq.combinations(2)
        for (pair <- pairs) {
          candidate_items += pair.toList.sorted
        }
      }

      else {
        val i = 0
        val j = 0
        for (i <- 0 to count - 2) {
          for (j <- i + 1 to count - 1) {
            breakable {
              val item1 = filter_freq_items_lis(i)
              val item2 = filter_freq_items_lis(j)
              if (item1.slice(0, size - 2) == item2.slice(0, size - 2)) {
                val union_set = item1.toSet.union(item2.toSet)
                candidate_items += union_set.toList.sorted
              }

              else {
                break
              }
            }
          }
        }
      }


      for (candidate <- candidate_items) {
        val scandidate = candidate.toSet
        val tcandidate = candidate.sorted
        for (basket <- local_baskets) {
          if (scandidate.subsetOf(basket)) {
            items_counter.update(tcandidate, items_counter(tcandidate) + 1)
          }
        }
      }

      for ((k, v) <- items_counter) {
        if (v >= partition_support) {
          true_freq_items += k
        }
      }

      filter_freq_items_lis = true_freq_items.toList
      partition_result ++= filter_freq_items_lis
      size += 1


    }

    return partition_result.iterator

  }


  def sort_baskets_bysize(basket1: List[Long], basket2: List[Long]): Boolean = {
    if (basket1.length > basket2.length) {
      return false

    } else {
      if (basket1.length < basket2.length) {
        return true
      }
      return sort_baskets_byelements(basket1, basket2)
    }
  }

  def sort_baskets_byelements(basket1: List[Long], basket2: List[Long]): Boolean = {
    if ((basket1.isEmpty && basket2.isEmpty) || (basket1.isEmpty)) {
      return true
    }

    if (basket2.isEmpty) {
      return false
    }

    var iter = 0
    while (iter < basket1.length && iter < basket2.length) {
      var value = basket1(iter).compareTo(basket2(iter))
      if (value != 0) {
        if (value > 0) {
          return false
        }
        return true
      }
      iter = iter + 1
    }

    return true
  }



  def main(args: Array[String]): Unit = {

    val start_time = System.currentTimeMillis()

    val conf: SparkConf = new SparkConf().setAppName("swathi_nayak_task1").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    val threshold = args(0).toInt
    val support = args(1).toInt
    val input_file = args(2)
    val output_file = args(3)

    /*
    val support = 50
    val threshold = 20
    val input_file = "/Users/swathinayak/IdeaProjects/HW2/src/main/scala/ta_feng_all_months_merged.csv"
    val output_file = "/Users/swathinayak/IdeaProjects/HW2/src/main/scala/case_print.txt"
    */


    val interim_file = Paths.get(output_file).getParent + "/customer_product.csv"
    // println(interim_file)

    var raw_data = sc.textFile(input_file)
    val raw_header = raw_data.first()

    val raw_baskets = raw_data.filter(row => row != raw_header).map(row => (row.split(",")(0).replace("\"","") + "-" + row.split(",")(1).replace("\"",""), row.split(",")(5).replace("\"","").toLong)).collect()

    // write to file
    val w1: Writer = {
      new FileWriter(interim_file)
    }
    try {
      w1.write("DATA-CUSTOMER_ID,PRODUCT_ID")
      raw_baskets.foreach(x => w1.write("\n"+ x._1 + "," + x._2))
      w1.close()
    } catch {
      case e: IOException => e.printStackTrace()
    }

    var rdd = sc.textFile(interim_file)
    var header = rdd.first()

    var baskets: RDD[Set[Long]] = null
    // println("Proceeding...")
    baskets = rdd.filter(row => row != header).map(row => row.split(",")).map(x => (x(0).toString, x(1).toLong)).groupByKey().map(_._2.toSet).filter(x => x.size > threshold)


    val total_baskets = baskets.count().toInt
    var local_freq_items = baskets.mapPartitions(data => local_frequent_items (data, support, total_baskets)).map(x=>(x,1)).reduceByKey((x,y)=>1).map(_._1).collect()
    // local_freq_items.foreach(println)
    // println(local_freq_items.length)
    val broadcasted = sc.broadcast(local_freq_items)

    val global_freq_items = baskets.mapPartitions(data => global_frequent_items (data, broadcasted)).reduceByKey(_+_).filter(_._2 >= support)
    val frequentitems = global_freq_items.map(_._1).collect()
    // global_freq_items.foreach(println)
    print(frequentitems.length)


    var printer = new PrintWriter(new File(output_file))

    var sorted_candidate_items = frequentitems.sortWith((x,y) => sort_baskets_bysize(x,y))
    var sorted_freq_items = frequentitems.sortWith((x,y) => sort_baskets_bysize(x,y))

    var data:String = ""

    var header_1 = "Candidates: \n"
    printer.write(header_1)


    if(sorted_candidate_items.length !=0){
      var len_1 = sorted_candidate_items(0).length
      var data = "('" + sorted_candidate_items(0)(0) + "')"

      printer.write(data)
      for(x <- 1 to sorted_candidate_items.length-1){
        var len_2 = sorted_candidate_items(x).length
        if(len_1 == len_2){
          printer.write(", ")
        }else{
          printer.write("\n\n")
        }
        if (len_2 == 1){
          data = "('" + sorted_candidate_items(x)(0) + "')"
        }else{
          data = ""
          for(k <- sorted_candidate_items(x)){
            if (k == sorted_candidate_items(x).last){
              data = data + "'"+ k + "'"
            }else{
              data = data + "'"+ k + "', "
            }
          }
          data = "(" + data + ")"
        }
        printer.write(data)
        len_1 = len_2
      }
      printer.write("\n\n")
    }

    var header_2 = "Frequent Itemsets: \n"
    printer.write(header_2)

    if(sorted_freq_items.length !=0){
      var len_1 = sorted_freq_items(0).length
      var data = "('" + sorted_freq_items(0)(0) + "')"

      printer.write(data)
      for(x <- 1 to sorted_freq_items.length-1){
        var len_2 = sorted_freq_items(x).length
        if(len_1 == len_2){
          printer.write(", ")
        }else{
          printer.write("\n\n")
        }
        if (len_2 == 1){
          data = "('" + sorted_freq_items(x)(0) + "')"
        }else{
          data = ""
          for(k <- sorted_freq_items(x)){
            if (k == sorted_freq_items(x).last){
              data = data + "'"+ k + "'"
            }else{
              data = data + "'"+ k + "', "
            }
          }
          data = "(" + data + ")"
        }
        printer.write(data)
        len_1 = len_2
      }
      printer.write("\n\n")
    }

    printer.close()

    val end_time = System.currentTimeMillis()
    println("Duration: " + (end_time - start_time)/1000 + " seconds")



  }

}
