package sql

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by c_rmacha on 4/18/2016.
 */
object DFrames {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("SQL TEST").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.read.json("C:\\winutils\\people.json")
    df.show()
    df.printSchema()
    df.first()
    df.select("name").show()

    df.select(df("name"),df("age")+1).show()

    df.filter(df("age")>1).show()

    df.groupBy("age").count().show()
  }

}
