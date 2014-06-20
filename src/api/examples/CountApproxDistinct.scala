package api.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object CountApproxDistinct {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "CountApproxDistinct Test")
    
    val a = sc.parallelize(1 to 10000, 20)
    val b = a++a++a++a++a
    
    val result = b.countApproxDistinct(0.1)
    println(result)
    //println(b.countApproxDistinct(0.05))
    //println(b.countApproxDistinct(0.01))
    //println(b.countApproxDistinct(0.001))
    
  }
}