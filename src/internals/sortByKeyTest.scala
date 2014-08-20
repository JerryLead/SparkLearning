package internals

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object sortByKeyTest {
  def main(args: Array[String]) {

    val sc = new SparkContext("local", "sortByKey Test")
    val data1 = Array[(Char, Int)](('A', 5), ('B', 4),
      ('C', 3), ('B', 2),
      ('C', 1), ('D', 2),
      ('C', 3), ('A', 4))
    val pairs = sc.parallelize(data1, 3)

    val result = pairs.sortByKey(true, 2)
    pairs.foreachWith(i => i)((x, i) => println("[pairsPartitionIndex " + i + "] " + x))
    result.foreachWith(i => i)((x, i) => println("[PartitionIndex " + i + "] " + x))
    
    println(result.toDebugString)
  }

}

/*
[pairsPartitionIndex 0] (A,5)
[pairsPartitionIndex 0] (B,4)
 
[pairsPartitionIndex 1] (C,3)
[pairsPartitionIndex 1] (B,2)
[pairsPartitionIndex 1] (C,1)

[pairsPartitionIndex 2] (D,2)
[pairsPartitionIndex 2] (C,3)
[pairsPartitionIndex 2] (A,4)

[PartitionIndex 0] (A,5)
[PartitionIndex 0] (A,4)
[PartitionIndex 0] (B,4)
[PartitionIndex 0] (B,2)

[PartitionIndex 1] (C,3)
[PartitionIndex 1] (C,1)
[PartitionIndex 1] (C,3)
[PartitionIndex 1] (D,2)

MapPartitionsRDD[4] at sortByKey at sortByKeyTest.scala:16 (2 partitions)
  ShuffledRDD[3] at sortByKey at sortByKeyTest.scala:16 (2 partitions)
    ParallelCollectionRDD[0] at parallelize at sortByKeyTest.scala:14 (3 partitions)
 */
