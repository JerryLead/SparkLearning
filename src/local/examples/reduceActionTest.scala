package local.examples

import org.apache.spark.SparkContext

object reduceActionTest {
  def main(args: Array[String]) {
    
    val sc = new SparkContext("local", "MapPartitionsRDD Test") 
	val data1 = Array[(String, Int)](("K1", 1), ("K2", 2),
		    						 ("U1", 3), ("U2", 4),
		    						 ("W1", 3), ("W2", 4)
		    							)    							
	val pairs = sc.parallelize(data1, 3)
	//val result = pairs.reduce((A, B) => (A._1 + "#" + B._1, A._2 + B._2))
	val result = pairs.fold(("K0",10))((A, B) => (A._1 + "#" + B._1, A._2 + B._2))
	println(result)
  }

}