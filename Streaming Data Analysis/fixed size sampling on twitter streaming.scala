// package com.datamining.hw5

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.Status
import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.collection.mutable.Map
import scala.util.control.Breaks._

object swathi_nayak_task3 {

  var sequence = 0
  val sample_size = 150
  var tweets_data = new ListBuffer[Status]()
  // var sampleTweetLength = 0
  var tag_counter = Map[String, Int]()


  def reservoir_sampling(tweet_chunk : RDD[Status]) : Unit = {

    val tweets = tweet_chunk.collect()
    for(status <- tweets){

      val htags = status.getHashtagEntities().map(_.getText)
      if ( htags.nonEmpty ){
        sequence += 1
        println("The number of tweets with tags from the beginning: " + sequence.toString)
        if (sequence <= sample_size)
          {
            tweets_data.append(status)
            for(tag <- htags){
              if(tag_counter.contains(tag)){
                tag_counter(tag) += 1
              }
              else{
                tag_counter(tag) = 1
              }
            }

            val freq_tags = tag_counter.toSeq.sortWith(_._2 > _._2).sorted
            val size = freq_tags.size

            var count = 0
            var prev = 0
            breakable {
              for (i <- 0 until size) {
               if (freq_tags(i)._2 != prev) {
                 count += 1
               }
             prev = freq_tags(i)._2
                println(freq_tags(i)._1 + " : " + freq_tags(i)._2)
                                if (count == 5) {
                                  break
                                }
              }
            }
            print("\n")

          }
        else {
          val tweet_seq = Random.nextInt(sequence)
          if (tweet_seq < sample_size) {
            val replace_tweet = tweets_data(tweet_seq)
            tweets_data(tweet_seq) = status
            val old_tags = replace_tweet.getHashtagEntities().map(_.getText)
            for (tag <- old_tags) {
              if (tag_counter(tag) > 1) {
                tag_counter(tag) -= 1
              }
              else{
                tag_counter -= tag
              }
            }

            val new_tags = status.getHashtagEntities().map(_.getText)
            for (tag <- new_tags) {
              if (tag_counter.contains(tag)) {
                tag_counter(tag) += 1
              }
              else {
                tag_counter(tag) = 1
              }
            }


          }
          val freq_tags = tag_counter.toSeq.sortWith(_._2 > _._2).sorted
          val size = freq_tags.size

          var count = 0
          var prev = 0
          breakable {
            for (i <- 0 until size) {

              if (freq_tags(i)._2 != prev) {
                count += 1
              }
              prev = freq_tags(i)._2
              println(freq_tags(i)._1 + " : " + freq_tags(i)._2)
              if (count == 5) {
                break
              }
            }
          }
          print("\n")



        }
        }
      }


  }

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("swathi_nayak_task3").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel(logLevel = "OFF")
    val api_key_1 = "ClUcrlPdQSfJ2WQ8O2AYubcYO"
    val api_key_2 = "87Qzrh3lrukaOjCEaafG7CznzfZ8ggnQeTemNQMCZPu333Q5rQ"
    val access_token_1 = "1200989670923833344-zCMdKlph30iFBPQB8PAkhg3LGeoqSe"
    val access_token_2 = "3ZTTJX31bv0YUkU9BAhMrr1Agji0WmYyzi79OZjdZQDBU"
    System.setProperty("twitter4j.oauth.consumerKey", api_key_1)
    System.setProperty("twitter4j.oauth.consumerSecret", api_key_2)
    System.setProperty("twitter4j.oauth.accessToken", access_token_1)
    System.setProperty("twitter4j.oauth.accessTokenSecret", access_token_2)
    val stream_ctxt = new StreamingContext(sc, Seconds(10))
    val tweet_stream = TwitterUtils.createStream(stream_ctxt, None, Array("#"))
    tweet_stream.foreachRDD(tweet_chunk => reservoir_sampling(tweet_chunk))
    stream_ctxt.start()
    stream_ctxt.awaitTermination()
  }
}

