// package com.datamining.hw5

import java.io.{File, PrintWriter}
import java.util
import java.io.FileOutputStream
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.{DefaultFormats, JObject}
import org.json4s.jackson.JsonMethods.parse
import java.util.Calendar
import java.text.SimpleDateFormat


object swathi_nayak_task1 {

  def convertJsonToState(jsonStr: String): String = {
    implicit val jsonFormat = DefaultFormats
    val key = parse(jsonStr).asInstanceOf[JObject]
    val bus_state = (key \ "state").extract[String]
    bus_state
  }

  def hash1(num: Int): Int = {
    return (533*num+131) % 200
    // num % 200
  }

  def hash2(num: Int): Int = {
    return (373*num+87) % 200
    // num % 193
  }


  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("swathi_nayak_task1").setMaster("local")

    val port = args(0)
    val output_file = args(1)

    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.sparkContext.setLogLevel(logLevel = "OFF")
    val substream = ssc.socketTextStream("localhost", port.toInt)

    val n = 200
    var bloom_array: Array[Int] = new Array(n)
    var state_set : util.HashSet[String] = new util.HashSet[String]()
    var falsepositives : Double = 0
    var truenegatives : Double = 0


    val out_file = new PrintWriter(new File(output_file))
    val output_header = "Time,FPR\n"
    out_file.write(output_header)
    out_file.close()

    substream.repartition(1)
    substream.foreachRDD(myrdd => { var chunks: Array[String] = myrdd.collect()
      for(chunk <- chunks){
        val bus_state = convertJsonToState(chunk)
        val bus_int = bus_state.hashCode

        val a = math.abs(hash1(bus_int))
        val b = math.abs(hash2(bus_int))

        if((bloom_array(a) + bloom_array(b)) == 2) {

          if (!state_set.contains(bus_state)) {
            falsepositives += 1
          }
        }

        else {

          bloom_array(a) = 1
          bloom_array(b) = 1
          truenegatives += 1

        }
        state_set.add(bus_state)

      }

      val den = falsepositives + truenegatives
      if (den != 0) {
        val fpr = (falsepositives / den).toDouble
        val fw = new PrintWriter(new FileOutputStream(new File(output_file),true))
        val format = new SimpleDateFormat("y-M-d h:m:s")
        val currdate = format.format(Calendar.getInstance().getTime())
        fw.write(currdate + "," + fpr.toString + "\n")
        fw.close()
      }
    })

    ssc.start()
    ssc.awaitTermination()


  }
}

