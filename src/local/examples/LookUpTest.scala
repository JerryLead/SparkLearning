package local.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object LookUpTest {
  def main(args: Array[String]) {

    val sc = new SparkContext("local", "LookUp Test")
    
    val data = Array[(String, Int)](("A", 1), ("B", 2),
      ("B", 3), ("C", 4),
      ("C", 5), ("C", 6))
      
    val pairs = sc.parallelize(data, 3)

    val finalRDD = pairs.lookup("B")
    
    finalRDD.foreach(println)
    // output:
    // 2
    // 3
  }
}