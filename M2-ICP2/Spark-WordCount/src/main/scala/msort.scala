
import org.apache.log4j.{Level, Logger}
import org.apache.spark._


object msort {

  def main(args: Array[String]): Unit = {


    //Controlling log level

   // Logger.getLogger("org").setLevel(Level.ERROR)
    //Logger.getLogger("akka").setLevel(Level.ERROR)

    //Spark Context

    val conf = new SparkConf().setAppName("msort").setMaster("local");
    val sc = new SparkContext(conf);

    val a = Array(19, 15, 16, 1, 14, 11, 8, 32);

    val b = sc.parallelize(a);


    val maparray = b.map(x => (x, 1))

    val sorted = maparray.sortByKey();

    println(sorted.foreach(println))

    sorted.keys.collect().foreach(println)
  }
}