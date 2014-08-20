package internals

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.RangePartitioner

object IntersectionTest {

   def main(args: Array[String]) {
    
    val sc = new SparkContext("local", "Intersection Test") 
    val a = sc.parallelize(List(1, 2, 3, 3, 4, 5), 3)
    val b = sc.parallelize(List(1, 2, 5, 6), 2)
    
    
    val r = a.intersection(b)
    
    a.foreachWith(i => i)((x, i) => println("[aIndex " + i + "] " + x))
    b.foreachWith(i => i)((x, i) => println("[bIndex " + i + "] " + x))
	r.foreachWith(i => i)((x, i) => println("[PartitionIndex " + i + "] " + x))
	
	println(r.toDebugString)

	/*
[aIndex 0] 1
[aIndex 0] 2

[aIndex 1] 3
[aIndex 1] 3

[aIndex 2] 4
[aIndex 2] 5

[bIndex 0] 1
[bIndex 0] 2

[bIndex 1] 5
[bIndex 1] 6

[PartitionIndex 1] 1

[PartitionIndex 2] 5
[PartitionIndex 2] 2

MappedRDD[7] at intersection at IntersectionTest.scala:16 (3 partitions)
  FilteredRDD[6] at intersection at IntersectionTest.scala:16 (3 partitions)
    MappedValuesRDD[5] at intersection at IntersectionTest.scala:16 (3 partitions)
      CoGroupedRDD[4] at intersection at IntersectionTest.scala:16 (3 partitions)
        MappedRDD[2] at intersection at IntersectionTest.scala:16 (3 partitions)
          ParallelCollectionRDD[0] at parallelize at IntersectionTest.scala:12 (3 partitions)
        MappedRDD[3] at intersection at IntersectionTest.scala:16 (2 partitions)
          ParallelCollectionRDD[1] at parallelize at IntersectionTest.scala:13 (2 partitions)
*/
  }
}