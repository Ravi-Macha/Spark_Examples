package examples

/**
 * Created by c_rmacha on 3/23/2016.
 */
import org.apache.spark._
object BasicAvgMapPartitions {
  case class AvgCount(var total: Int = 0, var num: Int = 0) {

    def merge(input: Iterator[Int]): AvgCount = {
     // println("Input we got is---"+input)
      input.foreach{elem =>
        total += elem
        num += 1
      }
      this
    }


    def merge(other: AvgCount): AvgCount = {
      println("Other we got is---=================="+other)
      total += other.total
      num += other.num
      this
    }

    def avg(): Float = {
      total / num.toFloat;
    }


  }

  def main(args: Array[String]) {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "BasicAvgMapPartitions", System.getenv("SPARK_HOME"))
    val input = sc.parallelize(List(1, 2, 3, 4))
    val intResult = input.mapPartitions(partition =>
      Iterator(AvgCount(0, 0).merge(partition)))

    println(".............................. "+intResult.collect().mkString(","))
    val result = intResult.reduce((x,y) => x.merge(y))
    println(result)
  }
}
