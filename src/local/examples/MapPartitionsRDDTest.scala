package local.examples

import org.apache.spark.SparkContext

object MapPartitionsRDDTest {

  def main(args: Array[String]) {
		val sc = new SparkContext("local", "MapPartitionsRDD Test") 
		val data = Array[(String, Int)](("A1", 1), ("A2", 2),
		    							("B1", 1), ("B2", 4),
		    							("C1", 3), ("C2", 4)
		    							)    							
		val pairs = sc.parallelize(data, 3)
		
		val finalRDD = pairs.mapPartitions(iter => iter.filter(_._2 >= 2))
		// val finalRDD2 = pairs.mapPartitionsWithIndex(f, preservesPartitioning)
		
		finalRDD.toArray().foreach(println)
		
  }
}