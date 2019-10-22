import org.apache.log4j.{Level, Logger}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.annotation.tailrec

object MergeSorting {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    //Controlling log level
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    //Spark Context
    val conf = new SparkConf().setAppName("MergeSort").setMaster("local");
    val sc = new SparkContext(conf);
    val list = List(List(38,27,43,3,9,82,10));
    val b = sc.parallelize(list)
    def mergeSort(xs: List[Int]): List[Int] = {
      val n = xs.length / 2
      if (n == 0) xs
      else {
        def merge(xs: List[Int], ys: List[Int]): List[Int] =
          (xs, ys) match {
            case(Nil, ys) => ys
            case(xs, Nil) => xs
            case(x :: xs1, y :: ys1) =>
              if (x < y) x::merge(xs1, ys)
              else y :: merge(xs, ys1)
          }
        val (left, right) = xs splitAt(n)
        merge(mergeSort(left), mergeSort(right))
      }
    }
    val r = b.map(mergeSort)
    println(r.collect().toList)
//    sorted.keys.saveAsTextFile("Output");
  }
}