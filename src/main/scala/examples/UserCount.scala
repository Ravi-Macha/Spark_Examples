package examples

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by c_rmacha on 4/23/2016.
 */
object UserCount {

  def main(args: Array[String]) {
    val Regex = """^.*Reputation="([0-9]+)" CreationDate="([0-9]{4}-[0-9]{2}-[0-9]{2})T.*" DisplayName="(.+)" LastAccessDate="(.+)" .*$""".r

    def processRow(row: String) = row match {
      case Regex(reputation, creationDate, displayName, lastAccess) => Some(User(displayName, reputation.toInt))
      case _ => None
    }

    val sparkConf = new SparkConf().setAppName("Read File").setMaster("local[1]")
    val sc = new SparkContext(sparkConf)
    val allUsersRDD = sc.textFile("C:\\Users\\c_rmacha\\Downloads\\stackoverflow.com-Users\\Users_Small.XML").persist(StorageLevel.DISK_ONLY)
    //val allUsersRDD = sc.textFile(args(0)).persist(StorageLevel.DISK_ONLY)

    val convertedToMap = allUsersRDD.map(processRow)
    // Some(User(Community,1))
    // Some(User(Jeff Atwood,36925))
    // Some(User(Geoff Dalgas,2499))
    println("Created another RDD which data structure we need......"+convertedToMap.collect().mkString(","))

    val top5 = convertedToMap.collect {
      case Some(user) => user.reputation -> user.displayName
    }.sortByKey(ascending = false).take(3)

    top5.foreach {
      case (reputation, displayName) => println("%s ===>>> %d points".format(displayName, reputation))
    }

  }

}
