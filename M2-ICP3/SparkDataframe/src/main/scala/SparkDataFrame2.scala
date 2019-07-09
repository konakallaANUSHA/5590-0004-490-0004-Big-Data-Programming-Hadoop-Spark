import org.apache.spark._
import org.apache.spark.sql.Row

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types._
object SparkDataFrame2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("my app")
    val sc = new SparkContext(conf)
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()


    import spark.implicits._


    val df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("/Users/anushakonakalla/Downloads/survey.csv")

    //df.show(60,!false)
    df.collect.foreach(println)
    df.printSchema()




    //val oldd = spark.sql("SELECT Gender, age, wellness_program FROM people WHERE age BETWEEN 35 AND 45")
    //oldd.show()
    //val adults = spark.sql("SELECT Country, age, care_options FROM people WHERE age BETWEEN 20 AND 30")
    //adults.show()

    //val unionDf = (oldd).union(adults)
    //unionDf.show()
    //unionDf.orderBy("Country").show()

    //13th Row from DataFrame
    val df1 = df.limit(5)
    df1.show()
    val df2 = df.limit(7)
    df2.show()
    val unionDf = df1.union(df2)
    unionDf.show()
    unionDf.orderBy("Country").show()

    //Save data to file
    println("\n Saved data to file")
    unionDf.write.parquet("/Users/anushakonakalla/Downloads/temp")


    df.createOrReplaceTempView("people")

    val DupDF = spark.sql("select COUNT(*),Country from people GROUP By Country Having COUNT(*) > 1")

    DupDF.show()

    //query based on group by using treatment column
    val treatment = spark.sql("select count(Country),treatment from people GROUP BY treatment ")
    treatment.show()

    //13th Row from DataFrame
    val df13th = df.take(13).last
    print(df13th)

    //aggregate
    //Aggregate Max and Average
    val MaxdF = spark.sql("select Max(Age) from people")
    MaxdF.show()

    val AvgdF = spark.sql("select Avg(Age) from people")
    AvgdF.show()


    val filterDF = df
        .select("Timestamp","Age","self_employed","Gender")
      .filter(($"self_employed" === "No") )
      .sort($"age".asc)
    filterDF.show()


    val filter2 = df
      .select("Timestamp","Country","family_history","wellness_program")
      .filter(($"self_employed" === "No") )
      .sort($"age".asc)
    filter2.show()


    //join query
    //val df3 = df.limit(50)
    //val df4 = df.limit(10)

    filter2.createOrReplaceTempView("left")


    filterDF.createOrReplaceTempView("right")


    val joinSQl = spark.sql("select left.Country, right.Age FROM left,right where left.Timestamp = " +
      "right.Timestamp")
    joinSQl.show()

    //df.write.partitionBy("Timestamp").saveAsTable("myparquet")

    def parseLine(line: String) =
    {
      val fields = line.split(",")

      val f1 = fields(0).toString

      val f2 = fields(1).toString
      val f3 = fields(3).toString
      (f1,f2,f3)
    }

    val lines = sc.textFile("/Users/anushakonakalla/Downloads/survey.csv")
    val rdd = lines.map(parseLine).toDF()
    rdd.show()


  }
}
