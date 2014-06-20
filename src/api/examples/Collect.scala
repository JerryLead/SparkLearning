package api.examples

import org.apache.spark.SparkContext

object Collect {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "Collect Test") 
    
    val c = sc.parallelize(List("Gnu", "cat", "Rat", "Dog", "Gnu", "Rat"), 2)
    
    val result = c.collect
    result.foreach(println)
  }
}