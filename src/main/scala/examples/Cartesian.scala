package examples

import org.apache.spark.{SparkContext, SparkConf}


/**
 * Created by c_rmacha on 4/23/2016.
 */
object Cartesian {


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Cart").setMaster("local")
    val sc = new SparkContext(conf)

    /*In a year boys with lacks of money*/

    val year_2014 = Array[(String, Int)](("Venky", 1), ("Shree", 2),("Ravi", 3), ("Shyam", 4),("shiv", 5), ("shiv", 6))
    val year_2015 = Array[(String, Int)](("Venky", 7), ("Shree", 8),("Ravi", 9), ("shiv", 0))

    val rdd1 = sc.parallelize(year_2014,3) // This creates a RDD partition of 3 {("Venky", 1), ("Shree", 2)}   ... {("Ravi", 3), ("Shyam", 4)} .... {("shiv", 5), ("shiv", 6)}
    val rdd2 = sc.parallelize(year_2015,2) //  This creates a RDD partition of 2 {("Venky", 7), ("Shree", 8)}....{("Ravi", 9), ("shiv", 0)}

    //var resultRDD = rdd1.cartesian(rdd2).mapValues(List(_)).reduceByKey( (x,y) => x ::: y)


  /*  val resultRDD = rdd1.cartesian(rdd2)
     /*Printing Cartesian Result*/
    //resultRDD.foreach(println)

    val countRdd = resultRDD.map{case((k1,v1),(k2,v2))=>(k1,v1+v2)}

    //countRdd.foreach(println)

    val r4 = resultRDD.flatMap(trans => Seq((trans, 1))).reduceByKey( _ + _).collect

    r4.foreach(println)

    /*Now to find how many people were having more money in lacks */

    def sumFunc(accum:Int, n:Int) =  accum + n*/

    //val p = resultRDD.mapValues(List(_)).reduceByKey( (x,y) => x ::: y)
    //println(p.collect)






  }

}
