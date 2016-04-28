package examples

import org.apache.spark.{RangePartitioner, SparkContext, SparkConf}

/**
 * Created by c_rmacha on 4/24/2016.
 */
object MapValuesTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Cart").setMaster("local")
    val sc = new SparkContext(conf)
    val data1 = Array[(String, Int)](("K", 1), ("T", 2),("T", 3), ("W", 4),("W", 5), ("W", 6))
    val pairs = sc.parallelize(data1, 3)


    // ("K", 1), ("T", 2)                     ("T", 3), ("W", 4)                      ("W", 5), ("W", 6)
    val result = pairs.reduce((A, B) => (A._1 + "#" + B._1, A._2 + B._2))
    println("----------------result------------------")
    result.productIterator.toList.foreach(println) // outputs K#T#T#W#W#W 21

    val result1 = pairs.fold(("R0",10))((A, B) => (A._1 + "#" + B._1, A._2 + B._2))
    println("----------------result2------------------")
    result1.productIterator.toList.foreach(println)


    val result3 = pairs.partitionBy(new RangePartitioner(2, pairs, true))
    println("----------------result3------------------")
    result3.foreach(println)

  }
}
