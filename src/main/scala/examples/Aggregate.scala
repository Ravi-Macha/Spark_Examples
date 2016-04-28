package examples
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by c_rmacha on 4/23/2016.
 */
object Aggregate {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Agg").setMaster("local")
    val sc = new SparkContext(conf)
    //val data = Array[(String,Int)](("A1",1), ("A2", 2), ("B1", 3), ("B2", 4), ("C1", 5), ("C2", 6))
    val data = Array[(String,Int)](("maths", 21),("english", 22),("science", 31))
    val pairs = sc.parallelize(data, 3)
    pairs.foreach(print)

    val result = pairs.aggregate(3) (
      (acc, value) => (acc + value._2),(acc1, acc2) => (acc1 + acc2)
    )
    println("Final Result - >"+result)

  }
}
