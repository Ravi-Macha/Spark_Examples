package examples

/**
 * Illustrates a simple map partition in Scala
 * @author Ravi Macha
 */
import org.apache.spark._
import org.eclipse.jetty.client.{ContentExchange, HttpClient}

object BasicMapPartitions {
    def main(args: Array[String]) {
      val master = args.length match {
        case x: Int if x > 0 => args(0)
        case _ => "local"
      }
      val sc = new SparkContext(master, "BasicMapPartitions", System.getenv("SPARK_HOME"))


      /******Example 1******/

      /*Creating the list of Search Keywords*/
      val input = sc.parallelize(List("UR5EPM", "DO1CR", "K1RI","EA1FCL"))
      val result = input.mapPartitions{signs=>
            val client = new HttpClient()
            client.start()
            val exchangeList = signs.map {sign =>
              val exchange = new ContentExchange(true);
              exchange.setURL(s"http://qrzcq.com/call/${sign}")
              client.send(exchange)
              exchange
          }
          exchangeList.map{exchange =>
              exchange.waitForDone();
              exchange.getResponseContent()
            }
      }
      println(result.collect().mkString(","))

      /******Example 2******/

      val myList = sc.parallelize(List("hello","dear","how","are","you"))
      val mapped = myList.mapPartitionsWithIndex{(index,iterator)=>{
        println("Called in Partition with Index ------- >" + index)
        val myMap = iterator.toList
        myMap.map(x => x + "->" + index).iterator
      }}
      println(mapped.collect().mkString(","))
    }
}
