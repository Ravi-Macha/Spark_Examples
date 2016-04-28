package examples

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by c_rmacha on 4/24/2016.
 */
object CollectAsMap {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Cart").setMaster("local")
    val sc = new SparkContext(conf)
    val year_2014 = Array[(String, Int)](("Venky", 1), ("Shree", 2),("Ravi", 3), ("Shyam", 4),("shiv", 5), ("shiv", 6))
    val pairs = sc.makeRDD(year_2014, 3)
    val result = pairs.flatMap(T => (T._1 + T._2))
    /*Removes Duplicates....*/
    result.foreach(println)
  }

}
