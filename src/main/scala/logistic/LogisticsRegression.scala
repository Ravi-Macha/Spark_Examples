package logistic

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * Created by c_rmacha on 4/4/2016.
 */
object LogisticsRegression {
  def main(args: Array[String]) {

    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local[2]"
    }

    val sparkConf = new SparkConf().setAppName("LogisticsRegression").setMaster(master)
    val sc = new SparkContext(sparkConf)
    val data = sc.textFile("C:\\Users\\c_rmacha\\Downloads\\stackoverflow.com-Users\\Qualitative_Bankruptcy.data.txt")
    println(data.count())


    def getDoubleValue(input: String): Double = input match {
      case "P" => 3.0
      case "A" => 2.0
      case "N" | "NB" => 1.0
      case "B" => 0.0
    }

    val parsedData = data.map{line =>
      val parts = line.split(",")
      LabeledPoint(getDoubleValue(parts(6)), Vectors.dense(parts.slice(0, 6).map(x => getDoubleValue(x))))
    }
    println(parsedData.collect())
    println(parsedData.count())




  }

}
