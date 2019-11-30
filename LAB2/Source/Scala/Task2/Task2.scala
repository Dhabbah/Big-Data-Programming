import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object test {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    //Controlling log level
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("task1").setMaster("local");
    val sc = new SparkContext(conf);
    // 1- Import the dataset and create data frames directly on import
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val WorldCupsDataset = sqlContext.read.option("header", "true")
      .format("csv")
      .csv("D:\\WorldCups.csv")
    //Create the Dataframe
    WorldCupsDataset.createTempView("WorldCups")
    val WorldCupMatchesDataset = sqlContext.read.option("header", "true")
      .format("csv")
      .csv("D:\\WorldCupMatches.csv")
    //Create the Dataframe
    WorldCupMatchesDataset.createTempView("Matches")
    val WorldCupPlayersDataset = sqlContext.read.option("header", "true")
      .format("csv")
      .csv("D:\\WorldCupPlayers.csv")
    //Create the Dataframe
    WorldCupPlayersDataset.createTempView("Players")

    //find who is the winner in the year of 2014 and 2010 in the WorldCup
    val q1 = sqlContext.sql("select Winner, Year from WorldCups where Year='2014' OR Year='2010' ")
    q1.show()

    //show how many matches had Italy played and their qualfied teams order by year
    val q2 = sqlContext.sql("select Year, Country, MatchesPlayed, QualifiedTeams from WorldCups WHERE Country = 'Italy' Order By Year")
    q2.show()

    //Find the second teams based on their matches played.
    val q3 = sqlContext.sql("select * from WorldCups where QualifiedTeams = (select MAX(QualifiedTeams) from WorldCups where QualifiedTeams < (select MAX(QualifiedTeams) from WorldCups ))")
    q3.show()

    //Show the records for the first and last year using Max and MIN in sql.
    val q4 = sqlContext.sql("select * from WorldCups where Year = (select MAX(Year) from WorldCups) OR Year = (select MIN(Year) from WorldCups)")
    q4.show()

    //Find how many times the cities hosted the world cup
    val q5 = sqlContext.sql("select City, Count(City) as times from Matches Group By City")
    q5.show()

    //show all the finals in 2014 and show their times, conditions, and attendances
    val q6 = sqlContext.sql("select Year, Stage, City, 'Win conditions', Attendance, Datetime from Matches where Stage = 'Final' AND Year = '2014' ")
    q6.show()

    //Find the number of all final matches
    val q7 = sqlContext.sql("select count(*) as number_of_matches from Matches where Stage='Final'")
    q7.show()

    //Find how many matches that held in Parque Central stadium
    val q8 = sqlContext.sql("select count(*) as number_of_matches from Matches where Stadium = 'Parque Central'")
    q8.show()

    //Find matches that Isidoro SOTA played with
    val q9 = sqlContext.sql("select * from Players where `Player Name` = 'Isidoro SOTA'")
    q9.show()

    //Find the Team initials for players who have position C
    val Q10 = sqlContext.sql("select `Team Initials`, `Player Name`, Position from Players where Position = 'C' ")
    Q10.show()

    //Find the stage of home teams by their years in order by the name
        val Q11 = sqlContext.sql("select Year, `Home Team Name`, Stage FROM Matches Group By Year, `Home Team Name`, Stage Order By `Home Team Name`")
        Q11.show()


    // Aggregate functions
    println("Aggregate functions")
    val Avg = sqlContext.sql("SELECT Avg(Attendance) as Average_Attendance FROM Matches")
    Avg.show()

    val Max = sqlContext.sql("SELECT Max(MatchesPlayed) as Max_Matches_Played FROM WorldCups")
    Max.show()

    val Min = sqlContext.sql("SELECT Min(MatchesPlayed) as Min_Matches_Played FROM WorldCups")
    Min.show()




    // RDD creation
    val csv = sc.textFile("D:\\WorldCups.csv")
    val h1 = csv.first()
    val data = csv.filter(line => line != h1)

    //find who is the winner in the year of 2014 and 2010 in the WorldCup using RDD
    val rdd1 = data.filter(line => line.split(",")(0) == "2010" || line.split(",")(0) == "2014")
      .map(line => (line.split(",")(2), (line.split(",")(0))) )
    println("RDD's output\n")
    rdd1.foreach(println)
    println("\nDataframe's output\n")
    WorldCupsDataset.select("Winner", "Year").filter("Year = 2010 or Year = 2014").show()

    //show how many matches had Italy played and their qualfied teams order by year
    val rdd2 = data.filter(line => (line.split(",")(1)=="Italy" ))
      .map(line => (line.split(",")(0),line.split(",")(1),line.split(",")(8),line.split(",")(7))).collect()
    println("RDD's output\n")
    rdd2.foreach(println)
    println("\nDataframe's output\n")
    WorldCupsDataset.select("Year","Country","MatchesPlayed","QualifiedTeams").filter("Country == 'Italy'").show(10)

    // Display the information about Winner who their QualifiedTeams equal to 24.
    val rdd3 = data.filter(line => (line.split(",")(7) == "24"))
      .map(line=> (line.split(",")(0),line.split(",")(1),line.split(",")(2), line.split(",")(3), line.split(",")(4),
        line.split(",")(5), line.split(",")(6), line.split(",")(7), line.split(",")(8), line.split(",")(9))).collect()
    println("RDD's output\n")
    rdd3.foreach(println)
    println("\nDataframe's output\n")
    WorldCupsDataset.select("Year","Country","Winner", "Runners-Up", "Third", "Fourth", "GoalsScored",
                                  "QualifiedTeams", "MatchesPlayed", "Attendance").filter("QualifiedTeams == 24").show()


    // Display the information that happened after 1950 where the Runners-Up is Netherlands
    val rdd4 = data.filter(line => line.split(",")(0) > "1950" && line.split(",")(3) == "Netherlands")
            .map(line=> (line.split(",")(0),line.split(",")(1),line.split(",")(2), line.split(",")(3), line.split(",")(4),
                                line.split(",")(5), line.split(",")(6), line.split(",")(7), line.split(",")(8), line.split(",")(9))).collect()
    println("RDD's output\n")
    rdd4.foreach(println)

    println("\nDataframe's output\n")
    WorldCupsDataset.select("*").filter("Year > 1950 and `Runners-Up` == 'Netherlands'").show()


    // Display the information about Runners-Up| who their GoalsScored more than 30
        val rdd5 = data.filter(line => line.split(",")(8) < "30")
                .map(line=> (line.split(",")(0),line.split(",")(6),line.split(",")(3))).collect()
        println("RDD's output\n")
        rdd5.foreach(println)
    println("\nDataframe's output\n")
    WorldCupsDataset.select("Year", "GoalsScored", "Runners-Up").filter("MatchesPlayed < 30").show()

  }

}
