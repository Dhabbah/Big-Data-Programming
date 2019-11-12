import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.functions.struct
import org.apache.spark.graphx._
import org.apache.spark.sql.functions._
import org.graphframes._

object Task1 {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    //Controlling log level
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("task1").setMaster("local");
    val sc = new SparkContext(conf);

    // 1-	Import the dataset as a csv file and create data frames directly on import than create graph out of the data frame created.
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // The First Dataset
    val TripDataset = sqlContext.read.option("header", "true")
      .csv("C:\\Users\\Michael\\Desktop\\UMKC\\Module 2\\Big Data Programming\\ICP5\\Datasets\\201508_trip_data.csv")
    TripDataset.createTempView("trip")

    // The Second Dataset
    val StationDataset = sqlContext.read.option("header", "true")
      .csv("C:\\Users\\Michael\\Desktop\\UMKC\\Module 2\\Big Data Programming\\ICP5\\Datasets\\201508_station_data.csv")
    StationDataset.createTempView("Station")



    // 2.	Concatenate chunks into list & convert to Data Frame
    // Using sql
    val Two1 = sqlContext.sql("select (lat , long) as Dist from Station")
//        Two.show()

    //Using struct function which creates a tuple of provided columns
    val Two2 = StationDataset.withColumn("Dist", struct(StationDataset("lat"), StationDataset("long")))
//        Two2.show()

    // Using array
    val Two3 = StationDataset.withColumn("Dist", array("lat", "long"))
    Two3.show()




    // 3.	Remove duplicates
    val StationData = StationDataset.distinct()
      .withColumnRenamed("station_id", "id")
//      StationData.show()

//    val StationData = StationDataset.dropDuplicates()
//        StationData.show()

    // 4.	Name Columns
    val TripData = TripDataset.withColumnRenamed("Start Terminal", "src")
                        .withColumnRenamed("End Terminal", "dst")
//    TripData.show()



    // 6.	Create vertices
    val Data = GraphFrame(StationData, TripData)

    // 7.	Show some vertices
    println("Total Number of Stations: " + Data.vertices.count)

    // 8.	Show some edges
    println("Total Number of Trips in Graph: " + Data.edges.count)

    // 9.	Vertex in-Degree
    val in_Degree = Data.inDegrees
    in_Degree.show(5)

    // 10.	Vertex out-Degree
    val out_Degree = Data.outDegrees
    out_Degree.show()

    // 11.	Apply the motif findings
    val motifs: DataFrame = Data.find("(a)-[e]->(b); (b)-[e2]->(a)")
    motifs.show()

    // Bonus: 1. Vertex degree
    val ver = Data.degrees
    ver.orderBy(desc("Degree")).limit(5)
    ver.show(5)

    // Bonus: 2. What are the most common destinations in the dataset from location to location?
    val topTrips = Data
      .edges
      .groupBy("src", "dst")
      .count()
      .orderBy(desc("count"))
      .limit(10)
    topTrips.show()

    // Bonus: 3. What is the station with the highest ratio of in degrees but fewest out degrees? As in, what station acts as almost a pure trip sink? A station where trips end at but rarely start from.
    val degreeRatio = in_Degree.join(out_Degree, in_Degree.col("id") === out_Degree.col("id"))
      .drop(out_Degree.col("id"))
      .selectExpr("id", "double(inDegree)/double(outDegree) as degreeRatio")
    degreeRatio.cache()

    println("Diaplay Ratio:" + degreeRatio.orderBy( desc("degreeRatio")).limit(10))
    degreeRatio.show()

    // Bonus: 4. Save graphs generated to a file.
//    degreeRatio.write.save("output")
//    degreeRatio.write.format("com.databricks.spark.csv").save("output1")

  }

}