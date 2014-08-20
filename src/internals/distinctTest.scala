package internals

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object distinctTest {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "distinct test")

    val pairs = sc.parallelize(List(1, 2, 2, 3, 2, 1, 4, 5), 3)

    val result = pairs.distinct(2)

    // output
    // [PartitionIndex 0] 1
    // [PartitionIndex 0] 2

    // [PartitionIndex 1] 2
    // [PartitionIndex 1] 3
    // [PartitionIndex 1] 2

    // [PartitionIndex 2] 1
    // [PartitionIndex 2] 4
    // [PartitionIndex 2] 5
    
    pairs.foreachWith(i => i)((x, i) => println("[PartitionIndex " + i + "] " + x))
    result.foreachWith(i => i)((x, i) => println("[PartitionIndex " + i + "] " + x))

    // output
    // [PartitionIndex 0] 4
    // [PartitionIndex 0] 2

    // [PartitionIndex 1] 1
    // [PartitionIndex 1] 3
    // [PartitionIndex 1] 5

    println(result.toDebugString)
  }

  /*
MappedRDD[5] at distinct at distinctTest.scala:12 (2 partitions)
  MapPartitionsRDD[4] at distinct at distinctTest.scala:12 (2 partitions)
    ShuffledRDD[3] at distinct at distinctTest.scala:12 (2 partitions)
      MapPartitionsRDD[2] at distinct at distinctTest.scala:12 (3 partitions)
        MappedRDD[1] at distinct at distinctTest.scala:12 (3 partitions)
          ParallelCollectionRDD[0] at parallelize at distinctTest.scala:10 (3 partitions)
   * 
   */
}