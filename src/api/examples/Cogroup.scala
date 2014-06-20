package api.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Cogroup {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "Cogroup Test")

    val a = sc.parallelize(List(1, 2, 1, 3), 1)
    val b = a.map((_, "b"))
    b.foreach(println)
    // output:
    // (1,b)
    // (2,b)
    // (1,b)
    // (3,b)
    val c = a.map((_, "c"))
    c.foreach(println)
    // output:
    // (1,c)
    // (2,c)
    // (1,c)
    // (3,c)
    
    val result = b.cogroup(c)
    result.foreach(println)
    
    // output:
    // (1,(ArrayBuffer(b, b),ArrayBuffer(c, c)))
    // (3,(ArrayBuffer(b),ArrayBuffer(c)))
    // (2,(ArrayBuffer(b),ArrayBuffer(c)))
  }
}