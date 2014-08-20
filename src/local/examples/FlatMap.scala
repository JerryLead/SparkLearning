package local.examples

import org.apache.spark.SparkContext

object FlatMap {
   def main(args: Array[String]) {
    
    val sc = new SparkContext("local", "FlatMap Test") 
	val data = Array[(String, Int)](("A", 1), ("B", 2),
		    						 ("B", 3), ("C", 4),
		    						 ("C", 5), ("C", 6)
		    						)    							
	val pairs = sc.makeRDD(data, 3)
	
	val result = pairs.flatMap(T => (T._1 + T._2))
	
	result.foreach(println)

  }
}