import org.apache.spark._
import org.apache.spark.streaming._



object log {

  def main(args: Array[String]): Unit = {

    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    val conf = new SparkConf().setMaster("local[2]").setAppName("log")

    //the checkpointed data would be rewritten every 10 seconds......checkpoint is nothing but cache but it stores on disk
    val ssc = new StreamingContext(conf, Seconds(5))

    //textFileStream can only monitor a folder when the files in the folder are being added or updated.
    val lines = ssc.textFileStream("/Users/anushakonakalla/Desktop/logs")

    val wc = lines.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_+ _)
    println(lines)
    wc.print(100)

    //    val words = lines.flatMap(_.split(" "))
    //
    //    // Count each word in each batch
    //    val pairs = words.map(word => (word, 1))
    //    val wordCounts = pairs.reduceByKey(_ + _)
    //
    //    // Print the first ten elements of each RDD generated in this DStream to the console
    //    wordCounts.print()
    // println(wordCounts)

    ssc.start()
    ssc.awaitTermination()


  }
}