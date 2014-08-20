package pretty.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.RangePartitioner

object CogroupPair {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "Cogroup Test")

    val data1 = Array[(String, Int)](("A", 1), ("A", 2),
    								 ("B", 3), ("B", 4),
    								 ("C", 5), ("C", 6))

    val data2 = Array[(String, Int)](("A", 7), ("A", 8),
    								 ("B", 9), ("C", 0))

    val data3 = Array[(String, Int)](("A", 10), ("B", 11))
      
    val pairs1 = sc.parallelize(data1, 3)
    val pairs2 = sc.parallelize(data2, 2)
    val pairs3 = sc.parallelize(data3, 3)

    val result1 = pairs1.cogroup(pairs2)
    result1.foreach(println)
    
//    val result2 = pairs1.cogroup(pairs2, pairs3)
//    result2.foreach(println)
//   
//    val result3 = pairs1.cogroup(pairs2, 1)
//    result3.foreachWith(i => i)((x, i) => println("[PartitionIndex " + i + "] " + x))
//    
//    val result4 = pairs1.cogroup(pairs2, new RangePartitioner(2, pairs1))
//    result4.foreachWith(i => i)((x, i) => println("[PartitionIndex " + i + "] " + x))
  }
}