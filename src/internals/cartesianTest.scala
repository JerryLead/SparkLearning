package internals

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object cartesianTest {
  def main(args: Array[String]) {

    val sc = new SparkContext("local", "cartesian Test")
    val data1 = Array[(Int, Char)]((1, 'a'), (2, 'b'),
      (3, 'c'), (4, 'd'))
    val pairs1 = sc.parallelize(data1, 2)

    val data2 = Array[(Int, Char)]((1, 'A'), (2, 'B'))
    val pairs2 = sc.parallelize(data2, 2)
    
    val result = pairs1.cartesian(pairs2)

    //pairs1.foreachWith(i => i)((x, i) => println("[pairs1-Index " + i + "] " + x))
    //pairs2.foreachWith(i => i)((x, i) => println("[pairs2-Index " + i + "] " + x))
    result.foreachWith(i => i)((x, i) => println("[PartitionIndex " + i + "] " + x))
    
     //println(result.toDebugString)
  }
}
/*
[pairs1-Index 0] (1,a)
[pairs1-Index 0] (2,b)

[pairs1-Index 1] (3,c)
[pairs1-Index 1] (4,d)

[pairs2-Index 0] (1,A)
[pairs2-Index 1] (2,B)

[PartitionIndex 0] ((1,a),(1,A))
[PartitionIndex 0] ((2,b),(1,A))

[PartitionIndex 1] ((1,a),(2,B))
[PartitionIndex 1] ((2,b),(2,B))

[PartitionIndex 2] ((3,c),(1,A))
[PartitionIndex 2] ((4,d),(1,A))

[PartitionIndex 3] ((3,c),(2,B))
[PartitionIndex 3] ((4,d),(2,B))



CartesianRDD[2] at cartesian at cartesianTest.scala:17 (4 partitions)
  ParallelCollectionRDD[0] at parallelize at cartesianTest.scala:12 (2 partitions)
  ParallelCollectionRDD[1] at parallelize at cartesianTest.scala:15 (2 partitions)

 */

