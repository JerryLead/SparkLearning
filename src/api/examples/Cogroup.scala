package api.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Cogroup {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "Cogroup Test")

    val a = sc.parallelize(List(1, 2, 1, 3), 2)
    val b = sc.parallelize(List(1, 2, 3, 4, 5, 6), 3)
    val d = a.map((_, "b"))
    //b.foreach(println)
    // output:
    // (1,b)
    // (2,b)
    // (1,b)
    // (3,b)
    val e = b.map((_, "c"))
    //c.foreach(println)
    // output:
    // (1,c)
    // (2,c)
    // (1,c)
    // (3,c)
    
    //val result = b.cogroup(c)
    val result = d.cogroup(e, 4)
    result.foreach(println)
    println(result.toDebugString)
    // output:
    // (1,(ArrayBuffer(b, b),ArrayBuffer(c, c)))
    // (3,(ArrayBuffer(b),ArrayBuffer(c)))
    // (2,(ArrayBuffer(b),ArrayBuffer(c)))
    
    /*
     * MappedValuesRDD[5] at cogroup at Cogroup.scala:28 (3 partitions)
     *  CoGroupedRDD[4] at cogroup at Cogroup.scala:28 (3 partitions)
     *    MappedRDD[2] at map at Cogroup.scala:12 (2 partitions)
     *      ParallelCollectionRDD[0] at parallelize at Cogroup.scala:10 (2 partitions)
     *    MappedRDD[3] at map at Cogroup.scala:19 (3 partitions)
     *      ParallelCollectionRDD[1] at parallelize at Cogroup.scala:11 (3 partitions)
     * 
     */
  }
}