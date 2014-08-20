package internals

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object pipeTest {
  def main(args: Array[String]) {

    val sc = new SparkContext("local", "cartesian Test")
    
    val a = sc.parallelize(1 to 9, 3) 
    val result = a.pipe("head -n 2")

    a.foreachWith(i => i)((x, i) => println("[aIndex " + i + "] " + x))
    result.foreachWith(i => i)((x, i) => println("[PartitionIndex " + i + "] " + x))
    
     println(result.toDebugString)
  }
}
/*
[PartitionIndex 0] 1
[PartitionIndex 0] 2

[PartitionIndex 1] 4
[PartitionIndex 1] 5

[PartitionIndex 2] 7
[PartitionIndex 2] 8



PipedRDD[1] at pipe at pipeTest.scala:12 (3 partitions)
  ParallelCollectionRDD[0] at parallelize at pipeTest.scala:11 (3 partitions)

 */

