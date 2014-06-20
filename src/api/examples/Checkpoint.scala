package api.examples

import org.apache.spark.SparkContext

object Checkpoint {
  def main(args: Array[String]) {
	  val sc = new SparkContext("local", "Checkpoint Test") 
	  
	  sc.setCheckpointDir("/Users/xulijie/Documents/data/checkpoint")
	  val a = sc.parallelize(1 to 4, 2)
	  a.checkpoint
	  a.count
  }
}