package logistic

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by c_rmacha on 4/6/2016.
 */
object SparkWordCount {
  def main(args:Array[String]) : Unit = {
    val sparkConf = new SparkConf().setAppName("Read File").setMaster("local[1]")
    val sc = new SparkContext(sparkConf)
    val tf = sc.textFile("C:\\spark-1.5.1\\spark-1.5.1\\README.md")
    //val tf = sc.textFile(args(0))
    val splits = tf.flatMap(line => line.split(" ")).map(word =>(word,1))
    val counts = splits.reduceByKey((x,y)=>x+y)
    splits.saveAsTextFile("C:\\spark-1.5.1\\spark-1.5.1\\SplitOutput")
    counts.saveAsTextFile("C:\\spark-1.5.1\\spark-1.5.1\\CountOutput")
  }
}
