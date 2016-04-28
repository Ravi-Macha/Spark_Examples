package examples

/**
 * Created by c_rmacha on 3/14/2016.
 */
import org.apache.spark.{SparkConf, SparkContext}

object CountWord {
  def main(args: Array[String]) {
    //System.setProperty("hadoop.home.dir", "c:\\winutil\\")
    val conf = new SparkConf().setAppName("SimpleApplication").setMaster("local")
    val sc = new SparkContext(conf)
    /*val logFile = "C:\\spark-1.6.0-bin-hadoop2.6\\README.md" // Should be some file on your system

    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter{line=>line.contains("a")}.count()
    val numBs = logData.filter(line=>line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))*/

    val x  = sc.parallelize(1 to 10, 3)
    println("........................."+x.flatMap(List.fill(scala.util.Random.nextInt(10))(_)).collect().mkString(","))

  }
}
