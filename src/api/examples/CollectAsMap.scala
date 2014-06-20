package api.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object CollectAsMap {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "CollectAsMap Test") 
    
    val a = sc.parallelize(List(1, 2, 1, 3), 1)
    val b = a.zip(a)
    
    val result = b.collectAsMap
    
    result.foreach(println)
    
    // output:
    // (2,2)
    // (1,1)
    // (3,3)
  }
}