package job


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map

/**
 * Created by c_rmacha on 3/28/2016.
 */


class ExampleJob(sc: SparkContext) {
  def run(transactionFile: String, userFile: String) : RDD[(String, String)] = {
    val transactions = sc.textFile(transactionFile)
    val newTransactionsPair = transactions.map{t =>
      val p = t.split("\t")
      println("After Split..........$p")
      (p(2).toInt, p(1).toInt)
    }

    val users = sc.textFile(userFile)
    val newUsersPair = users.map{t =>
      val p = t.split("\t")
      (p(0).toInt, p(3))
    }

    val result = processData(newTransactionsPair, newUsersPair)
    return sc.parallelize(result.toSeq).map(t => (t._1.toString, t._2.toString))
  }

  def processData (t: RDD[(Int, Int)], u: RDD[(Int, String)]) : Map[Int,Long] = {
    var jn = t.leftOuterJoin(u).values.distinct
    println("After Left outer join.........$jn "+jn)
    return jn.countByKey
  }
}


object ExampleJob {
  def main(args: Array[String]) {
    val usersFile = "C:\\sparkInput\\users.txt"
    val transactionsFile = "C:\\sparkInput\\transactions.txt"

    val outputPath =  "C:\\sparkInput\\output"

    val conf = new SparkConf().setAppName("SparkJoins").setMaster("local")
    val context = new SparkContext(conf)
    val job = new ExampleJob(context)
    val results = job.run(transactionsFile, usersFile)
    println("RESULTS......... "+results.collect())
    //results.saveAsTextFile(outputPath)
    context.stop()
  }

}
