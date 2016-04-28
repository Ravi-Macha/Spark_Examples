package examples

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by c_rmacha on 4/24/2016.
 */
object GroupByKey {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "GroupByKey Test")
    val data = Array[(Int, Char)]((1, 'a'), (2, 'b'),(3, 'c'), (4, 'd'),(5, 'e'), (3, 'f'),(2, 'g'), (1, 'h'))

    /*Create 3 parallel 3 partition*/
    val pairs = sc.parallelize(data, 3)

    /*2 is how many output partitions(Reducers) that you wants to create*/
    val result = pairs.groupByKey(2)
    result.foreach(println)
    result.foreachWith(i => i)((x, i) => println("[PartitionIndex " + i + "] " + x))
    //println(result.toDebugString)
  }

}
