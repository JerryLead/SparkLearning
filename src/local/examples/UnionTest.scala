package local.examples

import org.apache.spark.SparkContext

object UnionTest {
  def main(args: Array[String]) {
    
    val sc = new SparkContext("local", "ReduceByKeyToDriver Test") 
    
	val data1 = Array[(String, Int)](("K1", 1), ("K2", 2),
		    						 ("U1", 3), ("U2", 4),
		    						 ("W1", 5), ("W1", 6)
		    						)   
		    						
	val data2 = Array[(String, Int)](("K1", 7), ("K2", 8),
		    						 ("U1", 9), ("W1", 0)
		    						) 
	val pairs1 = sc.parallelize(data1, 3)
	val pairs2 = sc.parallelize(data2, 2)  
	//val result = pairs.reduce((A, B) => (A._1 + "#" + B._1, A._2 + B._2))
	//val result = pairs.fold(("K0",10))((A, B) => (A._1 + "#" + B._1, A._2 + B._2))
	//val result = pairs.partitionBy(new RangePartitioner(2, pairs, true))
	val result = pairs1.union(pairs2)
	result.foreach(println)
	//result.saveAsTextFile("E:\\Spark\\output\\join")
  }
}