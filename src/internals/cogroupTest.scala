package internals

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.RangePartitioner

object cogroupTest {

   def main(args: Array[String]) {
    
    val sc = new SparkContext("local", "cogroup Test") 
    val a = sc.parallelize(List(1, 2, 3, 3, 4, 5), 3).map(x => (x, 'a'))
    val b = sc.parallelize(List(1, 2, 5, 6), 2).map(y => (y, 'b'))
    
    
    val r = a.cogroup(b)
    
    a.foreachWith(i => i)((x, i) => println("[aIndex " + i + "] " + x))
    b.foreachWith(i => i)((x, i) => println("[bIndex " + i + "] " + x))
	r.foreachWith(i => i)((x, i) => println("[PartitionIndex " + i + "] " + x))
	
	println(r.toDebugString)

	/*
[aIndex 0] (1,a)
[aIndex 0] (2,a)

[aIndex 1] (3,a)
[aIndex 1] (3,a)

[aIndex 2] (4,a)
[aIndex 2] (5,a)

[bIndex 0] (1,b)
[bIndex 0] (2,b)

[bIndex 1] (5,b)
[bIndex 1] (6,b)

[PartitionIndex 0] (6,(ArrayBuffer(),ArrayBuffer(b)))
[PartitionIndex 0] (3,(ArrayBuffer(a, a),ArrayBuffer()))

[PartitionIndex 1] (4,(ArrayBuffer(a),ArrayBuffer()))
[PartitionIndex 1] (1,(ArrayBuffer(a),ArrayBuffer(b)))


[PartitionIndex 2] (5,(ArrayBuffer(a),ArrayBuffer(b)))
[PartitionIndex 2] (2,(ArrayBuffer(a),ArrayBuffer(b)))

MappedValuesRDD[5] at cogroup at cogroupTest.scala:16 (3 partitions)
  CoGroupedRDD[4] at cogroup at cogroupTest.scala:16 (3 partitions)
    MappedRDD[1] at map at cogroupTest.scala:12 (3 partitions)
      ParallelCollectionRDD[0] at parallelize at cogroupTest.scala:12 (3 partitions)
    MappedRDD[3] at map at cogroupTest.scala:13 (2 partitions)
      ParallelCollectionRDD[2] at parallelize at cogroupTest.scala:13 (2 partitions)
*/
  }
}