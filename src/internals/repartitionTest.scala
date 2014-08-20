package internals

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object repartitionTest {
  def main(args: Array[String]) {

    val sc = new SparkContext("local", "Coalesce Test")
    val y = sc.parallelize(1 to 100, 5)

    //y.foreach(println)

    val z = y.repartition(2)

    val r = z.takeOrdered(7)
    z.foreach(println)
  }
}