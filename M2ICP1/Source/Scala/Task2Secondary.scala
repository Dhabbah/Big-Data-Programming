import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.HashPartitioner

object Task2Secondary {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","C:\\winutils" )
    //val inputFile = args(0)
    //val outputFile = args(1)
    val conf = new SparkConf().setAppName("Task2Secondary").setMaster("local[*]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    // Load our input data.
    //val input =  sc.textFile(inputFile)
    val tempRDD = sc.textFile("sample2.txt")
    // Pairs the list of tuples.
    val pairsRDD = tempRDD.map(_.split(",")).map { k => ((k(0), k(1)),k(3))}.partitionBy(new HashPartitioner(2))
//    println("pairsRDD")
//    val listRDD = pairsRDD.partitionBy(new HashPartitioner(2))
    pairsRDD.foreach { println }
    val numReducers = 3;
    // partioning the created tuples and sorted.
    val listRDD = pairsRDD.groupByKey(numReducers).mapValues(iter => iter.toList.sortBy(k => k))
//    println("listRDD")
//    listRDD.foreach {
//      println
//    }
    listRDD.saveAsTextFile("Output3");
  }
}