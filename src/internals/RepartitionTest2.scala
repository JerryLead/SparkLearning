package internals

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.HashPartitioner

object RepartitionTest2 {
  def main(args: Array[String]) {

    val sc = new SparkContext("local", "repartition Test")
    val data = Array[(Int, Char)]((3, 'a'), (2, 'b'),
      (1, 'c'), (4, 'd'))
    val pairs1 = sc.parallelize(data, 3).partitionBy(new HashPartitioner(2))
    
    pairs1.foreachWith(i => i)((x, i) => println("[pairs1-Index " + i + "] " + x))
  }
}
/*
[pairs1-Index 0] (3,a)
[pairs1-Index 0] (2,b)
[pairs1-Index 0] (1,c)

[pairs1-Index 1] (4,d)
*/