package internals

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object broadcast {
  def main(args: Array[String]) {

    val bcName = "Http"
    val blockSize = "4096"

    System.setProperty("spark.broadcast.factory", "org.apache.spark.broadcast." + bcName +
      "BroadcastFactory")
    System.setProperty("spark.broadcast.blockSize", blockSize)
    val sparkConf = new SparkConf().setAppName("Broadcast Test").setMaster("local")

    val sc = new SparkContext(sparkConf)

    val slices = 2
    val num = 100

    val arr1 = new Array[Int](num)
  
    for (i <- 0 until arr1.length) {
      arr1(i) = i
    }
    
    val data = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

    val barr1 = sc.broadcast(arr1)
    val observedSizes = sc.parallelize(1 to 4, slices).map(_ => barr1.value.size)
    // Collect the small RDD so we can print the observed sizes locally.
    observedSizes.collect().foreach(i => println(i))

    //println(barr1.value.size)
    //barr1.value.collect
  }
}