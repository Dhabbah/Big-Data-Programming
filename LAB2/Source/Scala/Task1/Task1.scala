import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Task1 {

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","C:\\winutils" )
    //Controlling log level
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val SparkConfiguration = new SparkConf().setAppName("wordCount").setMaster("local[*]")
    // Create a Scala Spark Context.
    val SparkCont = new SparkContext(SparkConfiguration)
    // Load our input data.
    val FDataset =  "D:/facebook_combined.txt"
    val input = SparkCont.textFile(FDataset)
    // Split up into words.
    val c = input.map(line => line.split(" "))
      .filter(line=>(line.length == 2))
      .flatMap(line =>({
        val Friendslist = line(1).split(",");
        Friendslist.map(cf=>{
          if(line(0) < cf) ((line(0),cf),line(1))
          else ((cf,line(0)),line(1))
        })
      })
      )
    val CommonFriends = c.map(f=>((f._1._1.toInt, f._1._2.toInt), f._2.split(",").toSet))
      .reduceByKey((a,b)=>a.intersect(b))
      .sortByKey()
      .filter(z=>(z._2.size > 0))
      .map(f=>(f._1 + " => " + f._2))
    CommonFriends.collect().foreach(x=> println(x))
    // Save the word count back out to a text file
    CommonFriends.saveAsTextFile("output/output5")
  }


}