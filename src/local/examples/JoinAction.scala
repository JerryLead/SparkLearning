package local.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object JoinAction {
     def main(args: Array[String]) {
    
    val sc = new SparkContext("local[2]", "JoinAction Test") 
    
	val data1 = Array[(String, Int)](("A1", 1), ("A2", 2),
		    						 ("B1", 3), ("B2", 4),
		    						 ("C1", 5), ("C1", 6)
		    						)   
		    						
	val data2 = Array[(String, Int)](("A1", 7), ("A2", 8),
		    						 ("B1", 9), ("C1", 0)
		    						) 
	val pairs1 = sc.parallelize(data1, 3)
	val pairs2 = sc.parallelize(data2, 2)
	
	
	val result = pairs1.join(pairs2)
	
	// output:
	// (A1,(1,7))
	// (B1,(3,9))
	// (A2,(2,8))
	//
	// (C1,(5,0))
	// (C1,(6,0))
	result.foreach(println)
  }

}