package pretty.examples

import org.apache.spark.SparkContext

object Coalesce {
  def main(args: Array[String]) {
	  val sc = new SparkContext("local", "Coalesce Test") 
	  
	  val data = sc.parallelize(1 to 20, 10)
	  
	  val result = data.coalesce(2)
	  result.foreach(x => print(x + " "))
	  
	  // equals "repartition(2)" 
	  val resultWithHashPartition = data.coalesce(2, true)  
	  resultWithHashPartition.foreach(x => print(x + " "))
  }
}