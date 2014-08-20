package api.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.RangePartitioner

object GroupByKeyPair {

   def main(args: Array[String]) {
    
    val sc = new SparkContext("local", "GroupByKeyPair Test") 
    val d = sc.parallelize(1 to 100, 10)
    
	val pairs = d.keyBy(x => x % 10)
		   			
	val result1 = pairs.groupByKey()
	//val result2 = pairs.groupByKey(3)
	//val result3 = pairs.groupByKey(new RangePartitioner(3, pairs))
	
	println("Result 1:")
	result1.foreach(println)
	
	//println("Result 2:")
	//result2.foreach(println)
	
	//println("Result 3:")
	//result3.foreach(println)
	
  }
}