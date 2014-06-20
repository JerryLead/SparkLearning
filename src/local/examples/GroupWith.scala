package local.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object GroupWith {
  def main(args: Array[String]) {
    
    val sc = new SparkContext("local[2]", "GroupWith Test") 
    
	val data1 = Array[(String, Int)](("A1", 1), ("A2", 2),
		    						 ("B1", 3), ("B2", 4),
		    						 ("C1", 5), ("C1", 6)
		    						)   
		    						
	val data2 = Array[(String, Int)](("A1", 7), ("A2", 8),
		    						 ("B1", 9), ("C1", 0)
		    						) 
	val pairs1 = sc.parallelize(data1, 3)
	val pairs2 = sc.parallelize(data2, 2)

	val result = pairs1.groupWith(pairs2)
	result.foreach(println)
	
	// output:
	// (B1,(ArrayBuffer(3),ArrayBuffer(9)))
	// (A1,(ArrayBuffer(1),ArrayBuffer(7)))
	// (A2,(ArrayBuffer(2),ArrayBuffer(8)))
	//
	// (C1,(ArrayBuffer(5, 6),ArrayBuffer(0)))
	// (B2,(ArrayBuffer(4),ArrayBuffer()))
	
	
  }
}