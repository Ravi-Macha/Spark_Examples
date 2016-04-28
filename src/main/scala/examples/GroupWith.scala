package examples

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by c_rmacha on 4/24/2016.
 */
object GroupWith {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Cart").setMaster("local")
    val sc = new SparkContext(conf)

    /*In a year boys with lacks of money*/

    val year_2014 = Array[(String, Int)](("Venky", 1), ("Shree", 2),("Ravi", 3), ("Shyam", 4),("shiv", 5), ("shiv", 6))
    val year_2015 = Array[(String, Int)](("Venky", 7), ("Shree", 8),("Ravi", 9), ("shiv", 0))

    val rdd1 = sc.parallelize(year_2014,3) // This creates a RDD partition of 3 {("Venky", 1), ("Shree", 2)}   ... {("Ravi", 3), ("Shyam", 4)} .... {("shiv", 5), ("shiv", 6)}
    val rdd2 = sc.parallelize(year_2015,2) //  This creates a RDD partition of 2 {("Venky", 7), ("Shree", 8)}....{("Ravi", 9), ("shiv", 0)}

    val result = rdd1.groupWith(rdd2)
    result.foreach(println)
  }

}
