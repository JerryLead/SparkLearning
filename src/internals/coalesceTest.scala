package internals

import org.apache.spark.SparkContext

object coalesceTest {
  def main(args: Array[String]) {
	  val sc = new SparkContext("local", "Coalesce Test") 
	  
	  //val y = sc.parallelize(1 to 10, 5)
	  val y = sc.parallelize(List(1, 2, 3, 4, 5, 2, 5, 8, 3, 10), 5)
	  // y.foreachWith(i => i)((x, i) => println("[yPartitionIndex " + i + "] " + x))
	  
	  val z = y.coalesce(10, false)
	  
	  y.foreachWith(i => i)((x, i) => println("[yPartitionIndex " + i + "] " + x))
	  z.foreachWith(i => i)((x, i) => println("[zPartitionIndex " + i + "] " + x))
	  
	  println(z.toDebugString)
  }
}

/*
[yPartitionIndex 0] 1
[yPartitionIndex 0] 2

[yPartitionIndex 1] 3
[yPartitionIndex 1] 4

[yPartitionIndex 2] 5
[yPartitionIndex 2] 6

[yPartitionIndex 3] 7
[yPartitionIndex 3] 8

[yPartitionIndex 4] 9
[yPartitionIndex 4] 10

[zPartitionIndex 0] 1
[zPartitionIndex 0] 2

[zPartitionIndex 1] 3
[zPartitionIndex 1] 4
[zPartitionIndex 1] 5
[zPartitionIndex 1] 6

[zPartitionIndex 2] 7
[zPartitionIndex 2] 8
[zPartitionIndex 2] 9
[zPartitionIndex 2] 10


CoalescedRDD[1] at coalesce at coalesceTest.scala:13 (3 partitions)
  ParallelCollectionRDD[0] at parallelize at coalesceTest.scala:9 (5 partitions)
  
  
[zPartitionIndex 0] 6
[zPartitionIndex 0] 7
[zPartitionIndex 0] 9

[zPartitionIndex 1] 1
[zPartitionIndex 1] 3
[zPartitionIndex 1] 8
[zPartitionIndex 1] 10

[zPartitionIndex 2] 2
[zPartitionIndex 2] 4
[zPartitionIndex 2] 5




MappedRDD[4] at coalesce at coalesceTest.scala:13 (3 partitions)
  CoalescedRDD[3] at coalesce at coalesceTest.scala:13 (3 partitions)
    ShuffledRDD[2] at coalesce at coalesceTest.scala:13 (3 partitions)
      MapPartitionsRDD[1] at coalesce at coalesceTest.scala:13 (5 partitions)
        ParallelCollectionRDD[0] at parallelize at coalesceTest.scala:9 (5 partitions)


*/