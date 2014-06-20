package local.examples

import org.apache.spark.SparkContext

object PipedRDDTest {

  def main(args: Array[String]) {
		val sc = new SparkContext("local", "Cartesian Test") 
		val data1 = Array[(String, Int)](("K1", 1), ("K2", 2),
		    							("U1", 3), ("U2", 4),
		    							("W1", 3), ("W2", 4)
		    							)    							
		val pairs = sc.parallelize(data1, 3)
		
		val finalRDD = pairs.pipe("grep 2")
		
		finalRDD.foreach(println)
		
  }
}