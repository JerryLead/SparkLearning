package pretty.examples

import org.apache.spark.SparkContext

object Aggregate {
  
  def main(args: Array[String]) {

    val sc = new SparkContext("local", "Aggregate Test")
    val d = List("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")

    val data = sc.parallelize(d, 2)

    val result = data.aggregate("a")((x,y) => "[" + x + "," + y + "]", 
        (x,y) => x + y)
     
    println(result)
    // output:
    // a[[[[[a,0],1],2],3],4][[[[[a,5],6],7],8],9]
  }
}