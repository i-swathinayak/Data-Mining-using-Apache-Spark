// package com.datamining.hw5

import java.io.{File, FileOutputStream, PrintWriter}
import java.text.SimpleDateFormat
import java.util
import java.util.Calendar
import org.apache.spark.{SparkConf, streaming}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.{DefaultFormats, JObject}
import org.json4s.jackson.JsonMethods.parse
import scala.math.pow
import java.io.FileWriter



object swathi_nayak_task2 {


  def convertJsonToState(jsonStr: String): String = {
    implicit val jsonFormat = DefaultFormats
    val key = parse(jsonStr).asInstanceOf[JObject]
    val bus_state = (key \ "city").extract[String]
    bus_state
  }

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("swathi_nayak_task2").setMaster("local")

    val port = args(0)
    val output_file = args(1)

    val out_file = new PrintWriter(new File(output_file))
    val output_header = "Time,Ground Truth,Estimation\n"
    out_file.write(output_header)
    out_file.close()

    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel(logLevel = "OFF")
    val substream = ssc.socketTextStream("localhost", port.toInt)
    val wd = streaming.Duration(30000)
    val sd = streaming.Duration(10000)
    val streaming_window = substream.window(windowDuration = wd, slideDuration = sd)

    val n=12
    var len_trailing_zero: Array[Int] = new Array(n)
    var max_trailing_zero: Array[Int] = new Array(n)
    var city_set : util.HashSet[String] = new util.HashSet[String]()

    streaming_window.repartition(1)
    streaming_window.foreachRDD(myrdd => {myrdd.repartition(1)
      var chunks: Array[String] = myrdd.collect()
      for(chunk <- chunks){
      val bus_city = convertJsonToState(chunk)
      val bus_int = bus_city.hashCode
      val mod_vals = Array(17, 83, 29, 253, 97, 173, 53, 12, 37, 317, 79, 11)

      var bus_hash: Array[Int] = new Array(n)
      var bus_binary: Array[String] = new Array(n)

      for (i <- 0 to 11) {
        bus_hash(i) = bus_int % mod_vals(i)
      }

      for (i <- 0 to 11) {
        bus_binary(i) = bus_hash(i).toBinaryString
      }


      for (i <- 0 to 11) {
        val temp = bus_binary(i).reverse.dropWhile(_ == '0').reverse
        if ( bus_binary(i).length != (bus_binary(i).length - temp.length)) {
          len_trailing_zero(i) = bus_binary(i).length - temp.length
        }
      }

      for (i <- 0 to 11) {
        if (len_trailing_zero(i) > max_trailing_zero(i)) {
          max_trailing_zero(i) = len_trailing_zero(i)
        }
      }

      city_set.add(bus_city)

    }
      var grouped_averages: Array[Int] = new Array(6)

      for (i <- 0 until 11 by 2){
        grouped_averages(i/2) = (pow(2, max_trailing_zero(i)).toInt + pow(2, max_trailing_zero(i+1)).toInt)/2

      }

      grouped_averages = grouped_averages.sorted
      val estimate = grouped_averages(3).toString


      val fw = new PrintWriter(new FileOutputStream(new File(output_file),true))
      // val fw = new FileWriter(output_file, true)
      val format = new SimpleDateFormat("y-M-d h:m:s")
      val currdate = format.format(Calendar.getInstance().getTime())
      fw.write(currdate + "," + city_set.size().toString + "," + estimate + "\n")
      // fw.write(city_set.size().toString + "," + estimate + "\n")
      fw.close()
      // out_file.write(currdate + "," + city_set.size().toString + "," + estimate + "\n")

      })

    ssc.start()
    ssc.awaitTermination()

  }
}

