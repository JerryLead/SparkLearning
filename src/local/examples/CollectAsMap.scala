package local.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object CollectAsMap {
  def main(args: Array[String]) {

    val sc = new SparkContext("local", "CollectAsMap Test")
    val data = Array[(String, Int)](("A", 1), ("B", 2),
      ("B", 3), ("C", 4),
      ("C", 5), ("C", 6))
    
    // as same as "val pairs = sc.parallelize(data, 3)"
    val pairs = sc.makeRDD(data, 3)
    
    val result = pairs.collectAsMap

    // output Map(A -> 1, C -> 6, B -> 3)
    print(result)
  }

}