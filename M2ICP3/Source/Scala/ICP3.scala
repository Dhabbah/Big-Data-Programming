import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ICP3 {
  def main(args: Array[String]) {
  System.setProperty("hadoop.home.dir", "C:\\winutils")
    //Controlling log level
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("task1").setMaster("local");
    val sc = new SparkContext(conf);

    // 1- Import the dataset and create data frames directly on import
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val SCDataset = sqlContext.read.csv("C:\\Users\\Michael\\Desktop\\UMKC\\Module 2\\Big Data Programming\\ICP3\\Source Code\\Source Code\\survey.csv")
    val split = SCDataset.randomSplit(Array(1,1))
    val df1 =  split(0)
    val df2 = split(1)
    df1.createTempView("survey")
    df2.createTempView("survey1")



    // 2- Save data to file
        df1.write.save("output")
        df1.write.format("com.databricks.spark.csv").save("output1")


    // 3- Using query to find duplicate records
      println("Find duplicate records")
      val DuplicateDS = sqlContext.sql("select _c3, COUNT(*) as Number from survey GROUP By _c3 Having COUNT(*) > 1")
      DuplicateDS.show()

    // 4- Apply Union operation on the dataset and order the output by Country alphabetically.
    println("Using Union operation")
    val unionDf = df1.union(df2)
        unionDf.show()

//     OrderBy with column Country
    println("Ordering by country")
    val orderbycountry = sqlContext.sql("select * from survey ORDER BY _c7 ")
        orderbycountry.show()

    // 5- query based on group by using treatment column
    println("Group by using treatment")
    val treatment = sqlContext.sql("select count(_c7) as Number, _c7 from survey GROUP BY _c7 ")
    treatment.show()




    println("Inner Join")
    val joinSQL = sqlContext.sql("SELECT survey._c2,survey1._c3 FROM survey,survey1 where survey._c3 = " +
      "survey1._c3")
    joinSQL.show()

    //Inner Join
    println("Inner Join")
    val joinRight = sqlContext.sql("SELECT survey.*,survey1.* FROM survey INNER JOIN survey1 ON(survey._c3 = survey1._c3)")
    joinRight.show()


    // Aggregate functions
    println("Aggregate functions")
    val Avg = sqlContext.sql("SELECT Avg(_c1) as AverageAge FROM survey")
    Avg.show()

    val Max = sqlContext.sql("SELECT Max(_c1) as MaxAge FROM survey")
    Max.show()

    val Min = sqlContext.sql("SELECT Min(_c1) as MinAge FROM survey")
    Min.show()

    // Write a query to fetch 13th Row in the dataset
    println("Fetching 13th Row")
      val fetch3 = df1.take(13).head
    println(fetch3)
}

}

