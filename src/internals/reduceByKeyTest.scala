package internals

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object reduceByKeyTest {

  def main(args: Array[String]) {
    val sc = new SparkContext("local", "ReduceByKey Test")
    val data1 = Array[(String, Int)](("A", 1), ("B", 1),
      ("C", 1), ("B", 1),
      ("C", 1), ("D", 1),
      ("C", 1), ("A", 1))
    val pairs = sc.parallelize(data1, 3)
    
    // pairs.foreachWith(i => i)((x, i) => println("[pPartitionIndex " + i + "] " + x))
     
    // [pPartitionIndex 0] (A,1)
    // [pPartitionIndex 0] (B,1)
    
    // [pPartitionIndex 1] (C,1)
    // [pPartitionIndex 1] (B,1)
    // [pPartitionIndex 1] (C,1)
    
    // [pPartitionIndex 2] (D,1)
    // [pPartitionIndex 2] (C,1)
    // [pPartitionIndex 2] (A,1)
    
    //val result = pairs.reduce((A, B) => (A._1 + "#" + B._1, A._2 + B._2))
    //val result = pairs.fold(("K0",10))((A, B) => (A._1 + "#" + B._1, A._2 + B._2))
    val result = pairs.reduceByKey(_ + _, 2)
    result.foreachWith(i => i)((x, i) => println("[PartitionIndex " + i + "] " + x))

    println(result.toDebugString)
    
    // output
    // [PartitionIndex 0] (B,2)
    // [PartitionIndex 0] (D,1)
    // [PartitionIndex 1] (A,2)
    // [PartitionIndex 1] (C,3)
    
    /*
MapPartitionsRDD[3] at reduceByKey at reduceByKeyTest.scala:17 (2 partitions)
  ShuffledRDD[2] at reduceByKey at reduceByKeyTest.scala:17 (2 partitions)
    MapPartitionsRDD[1] at reduceByKey at reduceByKeyTest.scala:17 (3 partitions)
      ParallelCollectionRDD[0] at parallelize at reduceByKeyTest.scala:14 (3 partitions)
     */
  }
}