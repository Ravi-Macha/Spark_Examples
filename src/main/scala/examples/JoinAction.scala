package examples

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by c_rmacha on 4/24/2016.
 */
object JoinAction {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Cart").setMaster("local")
    val sc = new SparkContext(conf)
    val arr1 = Array[(String, Int)](("A", 1), ("B", 2), ("C", 3), ("D", 4))
    val arr2 = Array[(String, Int)](("A", 6), ("C", 2), ("x", 8), ("L", 3))

    val rdd1 = sc.parallelize(arr1)
    val rdd2 = sc.parallelize(arr2)

    /*Joins with Keys*/
    val result = rdd1.join(rdd2)
    result.foreach(println)
  }
}
