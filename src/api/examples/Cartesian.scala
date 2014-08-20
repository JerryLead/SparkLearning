package api.examples

import org.apache.spark.SparkContext

object Cartesian {
  def main(args: Array[String]) {
	  val sc = new SparkContext("local", "Cartesian Test") 
	  
	  val x = sc.parallelize(List(1, 2, 3, 4, 5))
	  val y = sc.parallelize(List(6, 7, 8, 9, 10))

	  println(x ++ y ++ x)
	  val result = x.cartesian(y)
	  //result.collect
	  result.foreach(println)
  }
}