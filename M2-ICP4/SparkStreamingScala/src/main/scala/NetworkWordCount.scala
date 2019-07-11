import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


object NetworkWordCount {
  def main(args: Array[String]): Unit = {

    // Create the context with a 1 second batch size
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")//special “local[*]” string to run in local mode

    val ssc = new StreamingContext(conf, Seconds(5))// Create a local StreamingContext with two working thread and batch interval of 5 second.

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)

    // Split each line into words
    val words = lines.flatMap(_.split(" "))

    // Count each word in each batch
    val wpairs = words.map(word => (word, 1))
    val wC = wpairs.reduceByKey(_ + _)
    //val wc = data.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)

    //wc.print()
    wC.print(100)

    ssc.start() // Start the computation

    ssc.awaitTermination()
  }
}

