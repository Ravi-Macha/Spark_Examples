package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, StreamingContext, Seconds}
import org.apache.spark.streaming.twitter.TwitterUtils

/**
 * Created by c_rmacha on 3/16/2016.
 */
object MyStreaming {
  def main(args: Array[String]) {
    if(args.length<4){
      println("Please do provide all keys to proceed....!!")
      System.exit(1)
    }
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val filters = args.takeRight(args.length - 4)

    val conf = new SparkConf().setMaster("local[2]").setAppName("MyStreaming")
    val ssc = new StreamingContext(conf, Seconds(2))
   // val stream = TwitterUtils.createStream(ssc, None, Seq())
    val stream = TwitterUtils.createStream(ssc, None, filters)

    val words = stream.flatMap(status => status.getText.split(" "))

    val hashTags = words.filter(_.startsWith("#"))
    println("......................  "+hashTags)
    //val hashTags = stream.flatMap(status => status.getHashtagEntities)
    //val hashTagPairs = hashTags.map(hashtag => ("#" + hashtag.getText, 1))
    // Use reduceByKeyAndWindow to reduce our hashtag pairs by summing their
    // counts over the last 10 seconds of batch intervals (in this case, 2 RDDs).
    //val wordCounts = hashTags.map((_, 1)).reduceByKeyAndWindow((a:Int,b:Int)=>a+b, Seconds(30), Seconds(10))

    //val wordCounts = hashTags.map(x => (x, 1L)).reduceByKeyAndWindow(_ + _,_ - _, Minutes(10), Seconds(2), 2)
    //wordCounts.print()

   val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))

    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))

    // Print popular hashtags
    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })
    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
