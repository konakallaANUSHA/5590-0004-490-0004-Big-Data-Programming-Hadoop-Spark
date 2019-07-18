import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame
import spire.syntax.order

object ICP6 {
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val spark = SparkSession
      .builder()
      .appName("ICP")
      .config("spark.master", "local")
      .getOrCreate()

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

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


    df1.show()

    df2.show()

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


    // Triangle Count

    val stationTraingleCount = stationGraph.triangleCount.run()
    stationTraingleCount.select("id","count").show()
    //stationTraingleCount.select("id","count" ).show()
    //stationTraingleCount.filter("id=Japantown").show()
    // Shortest Path
    val shortPath = stationGraph.shortestPaths.landmarks(Seq("MLK Library","Market at 4th")).run
    shortPath.show(false)

    //Page Rank

    val stationPageRank = stationGraph.pageRank.resetProbability(0.7).tol(0.01).run()
    stationPageRank.vertices.sort("pageRank").show()
   // stationPageRank.vertices.show()
    //stationPageRank.edges.sort("pageRank").show()

    val results3 = stationGraph.pageRank.resetProbability(0.15).maxIter(10).sourceId("Broadway St at Battery St").run()
    results3.vertices .show()



    val pathBFS = stationGraph.bfs.fromExpr("id = 'Harry Bridges Plaza (Ferry Building)'").toExpr("dockcount = 23").run()
    pathBFS.show(false)


    val result = stationGraph.labelPropagation.maxIter(5).run()
    (result.orderBy("label")).show()

   /* val result = ShortestPaths.run(graph = stationGraph)

    val shortestPath = result               // result is a graph
      .vertices                             // we get the vertices RDD
      .filter({case(vId, _) => vId == "MLK Library"})  // we filter to get only the shortest path from v1
      .first                                // there's only one value
      ._2                                   // the result is a tuple (v1, Map)
      .get("Japantown")*/





    stationGraph.vertices.write.csv("/Users/anushakonakalla/Desktop/ICP/vertices")

    stationGraph.edges.write.csv("/Users/anushakonakalla/Desktop/ICP/edges")









  }
}