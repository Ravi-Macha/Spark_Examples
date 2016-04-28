package sql

import org.apache.spark.{SparkContext, SparkConf}


/**
 * Created by c_rmacha on 4/18/2016.
 */

case class Person(name: String, age: Long)

object Dset {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("SQL TEST").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._



    val people = sc.textFile("C:\\winutils\\people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF()
    people.registerTempTable("people")

    val tenagers = sqlContext.sql("SELECT name, age from people WHERE age>13 and age<19");

    tenagers.map(t=>"Name :"+t(0)).collect().foreach(println)

    tenagers.map(t=>"Name in Diff format :"+t.getAs[String]("name")).collect().foreach(println)

  }
}
