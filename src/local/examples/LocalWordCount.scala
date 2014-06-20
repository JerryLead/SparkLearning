package local.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object LocalWordCount {
  def main(args: Array[String]) {
    
    val sc = new SparkContext("local[4]", "LocalWordCount")  
    val myFile = sc.textFile("/Users/xulijie/Documents/data/RandomText/randomText-10MB.txt")
    /*
    val counts = myFile.map( l => l.split(" ")(2) )
			.map( url => (url, 1) )
			.reduceByKey( _+_ )
			.map{ case(url, count) => (count, url) }
			.sortByKey( ascending=false )
			.map{ case(count, url) => (url, count) }
	
    */
    val wordAndCount = myFile.flatMap(s => s.split(" "))
    				   .map(w => (w, 1))
    				   
    val result = wordAndCount.reduceByKey(_ + _) 
    result.foreach(println)
   
  }

}