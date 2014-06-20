package local.examples

import org.apache.spark.SparkContext
import org.apache.spark.RangePartitioner

object GroupByAction {
  def main(args: Array[String]) {

    val sc = new SparkContext("local", "GroupByAction Test")

    val data = Array[(String, Int)](("A1", 1), ("A2", 2),
      ("B1", 6), ("A2", 4),
      ("B1", 3), ("B1", 5))

    val pairs = sc.parallelize(data, 3)
    
    // output: 
    // (A1,1)
    // (A2,2)
    //
    // (B1,6)
    // (A2,4)
    //
    // (B1,3)
    // (B1,5)
    pairs.foreach(println)

    val result1 = pairs.groupBy(K => K._1)
    val result2 = pairs.groupBy((K : (String, Int)) => K._1, 1)
    val result3 = pairs.groupBy((K : (String, Int)) => K._1, new RangePartitioner(3, pairs))
    
    // output of result1:
    // (A1,ArrayBuffer((A1,1)))
    //
    // (B1,ArrayBuffer((B1,6), (B1,3), (B1,5)))
    // (A2,ArrayBuffer((A2,2), (A2,4)))
    result1.foreach(println)
    
    // output of result2:
    // (A1,ArrayBuffer((A1,1)))
    // (B1,ArrayBuffer((B1,6), (B1,3), (B1,5)))
    // (A2,ArrayBuffer((A2,2), (A2,4)))
    result2.foreach(println)
    
    // output of result3:
    // (A1,ArrayBuffer((A1,1)))
    // (A2,ArrayBuffer((A2,2), (A2,4)))
    //
    // (B1,ArrayBuffer((B1,6), (B1,3), (B1,5)))
    result3.foreach(println)

  }

}