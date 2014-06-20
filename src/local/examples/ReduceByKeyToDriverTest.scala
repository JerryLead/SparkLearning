package local.examples

import org.apache.spark.SparkContext

object ReduceByKeyToDriverTest {
  def main(args: Array[String]) {
    
    val sc = new SparkContext("local[3]", "ReduceByKeyToDriver Test") 
	val data1 = Array[(String, Int)](("K", 1), ("U", 2),
		    						 ("U", 3), ("W", 4),
		    						 ("W", 5), ("W", 6)
		    						)    							
	val pairs = sc.parallelize(data1, 3)
	//val result = pairs.reduce((A, B) => (A._1 + "#" + B._1, A._2 + B._2))
	//val result = pairs.fold(("K0",10))((A, B) => (A._1 + "#" + B._1, A._2 + B._2))
	//val result = pairs.reduceByKeyToDriver(_ + _)
	//result.foreach(println)
  }
}