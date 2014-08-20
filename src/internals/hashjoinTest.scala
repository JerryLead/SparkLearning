package internals

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.HashPartitioner

object hashjoinTest {
  def main(args: Array[String]) {

    val sc = new SparkContext("local", "hashjoin Test")
    val data1 = Array[(Int, Char)]((1, 'a'), (2, 'b'),
      (3, 'c'), (4, 'd'),
      (5, 'e'), (3, 'f'),
      (2, 'g'), (1, 'h'))
    val pairs1 = sc.parallelize(data1, 3).partitionBy(new HashPartitioner(3))
    

    val data2 = Array[(Int, Char)]((1, 'A'), (2, 'B'),
      (3, 'C'), (4, 'D'))
    val pairs2 = sc.parallelize(data2, 2)
   
    val result = pairs1.join(pairs2)

    //pairs1.foreachWith(i => i)((x, i) => println("[pairs1-Index " + i + "] " + x))
    //pairs2.foreachWith(i => i)((x, i) => println("[pairs2-Index " + i + "] " + x))
    result.foreachWith(i => i)((x, i) => println("[PartitionIndex " + i + "] " + x))
    
    println(result.toDebugString)
  /*
[pairs1-Index 0] (1,a)
[pairs1-Index 0] (2,b)

[pairs1-Index 1] (3,c)
[pairs1-Index 1] (4,d)
[pairs1-Index 1] (5,e)

[pairs1-Index 2] (3,f)
[pairs1-Index 2] (2,g)
[pairs1-Index 2] (1,h)

[pairs2-Index 0] (1,A)
[pairs2-Index 0] (2,B)

[pairs2-Index 1] (3,C)
[pairs2-Index 1] (4,D)

[PartitionIndex 0] (3,(c,C))
[PartitionIndex 0] (3,(f,C))

[PartitionIndex 1] (4,(d,D))
[PartitionIndex 1] (1,(a,A))
[PartitionIndex 1] (1,(h,A))

[PartitionIndex 2] (2,(b,B))
[PartitionIndex 2] (2,(g,B))

FlatMappedValuesRDD[4] at join at joinTest.scala:20 (3 partitions)
  MappedValuesRDD[3] at join at joinTest.scala:20 (3 partitions)
    CoGroupedRDD[2] at join at joinTest.scala:20 (3 partitions)
      ParallelCollectionRDD[0] at parallelize at joinTest.scala:14 (3 partitions)
      ParallelCollectionRDD[1] at parallelize at joinTest.scala:18 (2 partitions)

   */
  }
}