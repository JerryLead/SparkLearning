package local.examples

import org.apache.spark.SparkContext

object AggregateOrder {

  def main(args: Array[String]) {

    val sc = new SparkContext("local", "AggregateOrder Test")
    val data = List("12", "23", "345", "4567")

    val pairs = sc.parallelize(data, 2)
    pairs.foreach(x => println(x.length))
    
    //val result = pairs.aggregate("")((x,y) => math.min(x.length, y.length).toString, (x,y) => x + y)
    
    val result2 = pairs.aggregate("")((x,y) => "[" + x.length + "," + y.length + "] ", (x,y) => x + y)
     
    result2.foreach(println)
    println(result2)

  }
}