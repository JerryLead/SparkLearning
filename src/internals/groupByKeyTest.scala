package internals

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object groupByKeyTest {

   def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GroupByKey").setMaster("local")
    val sc = new SparkContext(conf) 
    sc.setCheckpointDir("/Users/xulijie/Documents/data/checkpoint")
     
	val data = Array[(Int, Char)]((1, 'a'), (2, 'b'),
		    						 (3, 'c'), (4, 'd'),
		    						 (5, 'e'), (3, 'f'),
		    						 (2, 'g'), (1, 'h')
		    						 
		    						)    							
	val pairs = sc.parallelize(data, 3)
	
	pairs.checkpoint
	pairs.count
	
	val result = pairs.groupByKey(2)
	
	// output:
	//pairs.foreachWith(i => i)((x, i) => println("[dataPartitionIndex " + i + "] " + x))
	result.foreachWith(i => i)((x, i) => println("[PartitionIndex " + i + "] " + x))
	
	println(result.toDebugString)

	/*
[dataPartitionIndex 0] (1,a)
[dataPartitionIndex 0] (2,b)

[dataPartitionIndex 1] (3,c)
[dataPartitionIndex 1] (4,d)
[dataPartitionIndex 1] (5,e)

[dataPartitionIndex 2] (3,f)
[dataPartitionIndex 2] (2,g)
[dataPartitionIndex 2] (1,h)

[PartitionIndex 0] (4,ArrayBuffer(d))
[PartitionIndex 0] (2,ArrayBuffer(b, g))

[PartitionIndex 1] (1,ArrayBuffer(a, h))
[PartitionIndex 1] (3,ArrayBuffer(c, f))
[PartitionIndex 1] (5,ArrayBuffer(e))

MappedValuesRDD[3] at groupByKey at groupByKeyTest.scala:19 (2 partitions)
  MapPartitionsRDD[2] at groupByKey at groupByKeyTest.scala:19 (2 partitions)
    ShuffledRDD[1] at groupByKey at groupByKeyTest.scala:19 (2 partitions)
      ParallelCollectionRDD[0] at parallelize at groupByKeyTest.scala:17 (3 partitions)
*/
  }
}