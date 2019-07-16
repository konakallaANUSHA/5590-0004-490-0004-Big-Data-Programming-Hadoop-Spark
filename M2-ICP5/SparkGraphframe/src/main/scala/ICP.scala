import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame
import spire.syntax.order

object ICP {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession
      .builder()
      .appName("ICP")
      .config("spark.master", "local")
      .getOrCreate()

    val df1 = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("/Users/anushakonakalla/Downloads/201508_trip_data.csv")

    val df2 = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("/Users/anushakonakalla/Downloads/201508_station_data.csv")


    df1.collect.foreach(println)

      df2.collect.foreach(println)

    // Printing the Schema

    df1.printSchema()

    df2.printSchema()



    //First of all create three Temp View

    df1.createOrReplaceTempView("Trips")

    df2.createOrReplaceTempView("Stations")


    val nstation = spark.sql("select * from Stations")

    nstation.show()

    val ntrips = spark.sql("select * from Trips")

    ntrips.show()

    val stationVertices = nstation
      .withColumnRenamed("name", "id")
      .distinct()

    stationVertices.show(80)



    val tripEdges = ntrips
      .withColumnRenamed("Start Station", "src")
      .withColumnRenamed("End Station", "dst")

    tripEdges.show()

    val stationGraph = GraphFrame(stationVertices, tripEdges)

    tripEdges.cache()

    stationVertices.cache()

    println("Total Number of Stations: " + stationGraph.vertices.count)
    println("Total Number of Distinct Stations: " + stationGraph.vertices.distinct().count)
    println("Total Number of Trips in Graph: " + stationGraph.edges.count)
    println("Total Number of Distinct Trips in Graph: " + stationGraph.edges.distinct().count)
    println("Total Number of Trips in Original Data: " + ntrips.count)//

    stationGraph.vertices.show()

    stationGraph.edges.show()

    val inDeg = stationGraph.inDegrees

    println("InDegree" + inDeg.orderBy(desc("inDegree")).limit(5))
    inDeg.show(5)

    val outDeg = stationGraph.outDegrees
    println("OutDegree" + outDeg.orderBy(desc("outDegree")).limit(5))
    outDeg.show(5)


    val ver = stationGraph.degrees
    ver.show(5)
    println("Degree" + ver.orderBy(desc("Degree")).limit(5))

    val topTrips = stationGraph
      .edges
      .groupBy("src", "dst")
      .count()
      .orderBy(desc("count"))
      .limit(10)

    topTrips.show()


    val degreeRatio = inDeg.join(outDeg, inDeg.col("id") === outDeg.col("id"))
      .drop(outDeg.col("id"))
      .selectExpr("id", "double(inDegree)/double(outDegree) as degreeRatio")
        .sort()



    degreeRatio.cache()

    degreeRatio.show()
    //println(degreeRatio.orderBy(desc("degreeRatio")).limit(10))

    val motifs = stationGraph.find("(a)-[e]->(b); (b)-[e2]->(a)")

    motifs.show(50)

    //val motifs2 = stationGraph.find("a.id = San Antonio Shopping Center")

    //motifs2.show()

    stationGraph.vertices.write.csv("/Users/anushakonakalla/Desktop/ICP/vertices")

    stationGraph.edges.write.csv("/Users/anushakonakalla/Desktop/ICP/edges")









  }
}