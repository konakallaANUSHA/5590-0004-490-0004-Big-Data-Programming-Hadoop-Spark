/**
  * Illustrates flatMap + countByValue for wordcount.
  */


import org.apache.spark._

object try1 {
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","F:\\winutils" )
    //val inputFile = args(0)
    //val outputFile = args(1)
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    // Load our input data.
    //val input =  sc.textFile(inputFile)
    val input = sc.textFile("input.txt")
    // Split up into words.
    val words = input.flatMap(line => line.split(""))
    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}.sortByKey(true,1)
    //collect the distinct words and store in variable distinct
    val distinct = words.distinct()
    //prints the distinct variables by separator;
    println(distinct.collect().mkString(";"))
    //printing total count
    println("Total no of words are: ", words.count())
    println(counts.take(4).foreach(println))
    counts.saveAsTextFile("output1")
    // Save the word count back out to a text file, causing evaluation.

  }
}
