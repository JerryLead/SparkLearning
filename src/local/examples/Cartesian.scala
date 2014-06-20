package local.examples

import org.apache.spark.SparkContext

object Cartesian {
	def main(args: Array[String]) {
		val sc = new SparkContext("local", "Cartesian Test") 
		val data1 = Array[(String, Int)](("A1", 1), ("A2", 2),
		    							 ("B1", 3), ("B2", 4),
		    							 ("C1", 5), ("C1", 6))
		
		val data2 = Array[(String, Int)](("A1", 7), ("A2", 8),
		    							("B1", 9), ("C1", 0))    							
		val pairs1 = sc.parallelize(data1, 3)
		val pairs2 = sc.parallelize(data2, 2)
		
		val resultRDD = pairs1.cartesian(pairs2)
		
		resultRDD.foreach(println)
		
		/*
		 * Output of task1:
		 * ((A1,1),(A1,7))
		 * ((A1,1),(A2,8))
		 * ((A2,2),(A1,7))
		 * ((A2,2),(A2,8))
		 * Output of task2:
		 * ((A1,1),(B1,9))
		 * ((A1,1),(C1,0))
		 * ((A2,2),(B1,9))
		 * ((A2,2),(C1,0))
		 * Output of task3:
		 * ((B1,3),(A1,7))
		 * ((B1,3),(A2,8))
		 * ((B2,4),(A1,7))
		 * ((B2,4),(A2,8))
		 * Output of task4:
		 * ((B1,3),(B1,9))
		 * ((B1,3),(C1,0))
		 * ((B2,4),(B1,9))
		 * ((B2,4),(C1,0))
		 * Output of task5:
		 * ((C1,5),(A1,7))
		 * ((C1,5),(A2,8))
		 * ((C1,6),(A1,7))
		 * ((C1,6),(A2,8))
		 * Output of task6:
		 * ((C1,5),(B1,9))
		 * ((C1,5),(C1,0))
		 * ((C1,6),(B1,9))
		 * ((C1,6),(C1,0))
		 */
		
	}
}