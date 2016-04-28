package streaming

import org.apache.spark._
import org.apache.spark.rdd.RDD._

/**
 * Created by c_rmacha on 3/23/2016.
 */
object WordCount {
  def main(args: Array[String]) {
    val master =  args.length match {
      case x :Int if x > 0 => args(0)
      case _ => "local"
    }
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    val input = args.length match{
      case x :Int if x > 1 => args(1)
      case _ => sc.parallelize(List("Ravi", "Macha Bangalore"))
    }



  }
}
