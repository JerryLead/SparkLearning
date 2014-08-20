package local.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object GroupByKey {

   def main(args: Array[String]) {
    
    val sc = new SparkContext("local", "GroupByKey Test") 
	val data = Array[(Int, Char)]((1, 'a'), (2, 'b'),
		    						 (3, 'c'), (4, 'd'),
		    						 (5, 'e'), (3, 'f'),
		    						 (2, 'g'), (1, 'h')
		    						 
		    						)    							
	val pairs = sc.parallelize(data, 3)
	
	val result = pairs.groupByKey(2)
	
	// output:
	// (B,ArrayBuffer(2, 3))
	// 
	// (A,ArrayBuffer(1))
	// (C,ArrayBuffer(4, 5, 6))
	//result.foreach(println)
	result.foreachWith(i => i)((x, i) => println("[PartitionIndex " + i + "] " + x))
	println(result.toDebugString)
  }
}