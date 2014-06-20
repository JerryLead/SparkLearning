package local.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object GroupByKey {

   def main(args: Array[String]) {
    
    val sc = new SparkContext("local", "GroupByKey Test") 
	val data = Array[(String, Int)](("A", 1), ("B", 2),
		    						 ("B", 3), ("C", 4),
		    						 ("C", 5), ("C", 6)
		    						)    							
	val pairs = sc.parallelize(data, 3)
	
	val result = pairs.groupByKey(2)
	
	// output:
	// (B,ArrayBuffer(2, 3))
	// 
	// (A,ArrayBuffer(1))
	// (C,ArrayBuffer(4, 5, 6))
	result.foreach(println)
  }
}