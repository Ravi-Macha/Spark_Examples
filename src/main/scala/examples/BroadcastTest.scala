package examples

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Usage: BroadcastTest [slices] [numElem] [broadcastAlgo] [blockSize]
  */
object BroadcastTest {
  def main(args: Array[String]) {
    val bcName = if (args.length > 2) args(2) else "Http"
    val blockSize = if (args.length > 3) args(3) else "4096"
    val sparkConf = new SparkConf().setAppName("Broadcast Test").setMaster("local[1]").set("spark.broadcast.factory", s"org.apache.spark.broadcast.${bcName}BroadcastFactory").set("spark.broadcast.blockSize", blockSize)
    val sc = new SparkContext(sparkConf)

    val slices = if (args.length > 0) args(0).toInt else 1
    val num = if (args.length > 1) args(1).toInt else 1000000

    val arr1 = (0 until num).toList

    for (i <- 0 until 3) {
      println("Iteration " + i)
      println("===========")
      val startTime = System.nanoTime
      val barr1 = sc.broadcast(arr1)
      println("I have broadcasted one arrayvalue")
      val inter = sc.parallelize(1 to 10, slices)
      println("Done with this....  "+inter.collect())
      val observedSizes = inter.map(_ => barr1.value.size)
      // Collect the small RDD so we can print the observed sizes locally.
      observedSizes.collect().foreach(i => println(i))
      println("Iteration %d took %.0f milliseconds".format(i, (System.nanoTime - startTime) / 1E6))
    }

    sc.stop()
  }
}
// scalastyle:on println
