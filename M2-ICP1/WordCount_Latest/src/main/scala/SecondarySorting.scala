import org.apache.spark.{ SparkConf, SparkContext }



object SecondarySorting {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","C:\\winutils" )
    val conf = new SparkConf().setAppName("secondarysorting").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val personRDD = sc.textFile("input1.txt")
    val pairsRDD = personRDD.map(_.split(",")).map { k => ((k(0), k(1)),k(3))}


    println(pairsRDD.foreach(println))
    //println(pairsRDD.foreach(println))
    val numReducers = 3;

    val listRDD = pairsRDD.groupByKey(numReducers).mapValues(iter => iter.toList.sortBy(k => k)).sortByKey(true,1)
    println(listRDD.foreach(println))


    //listRDD.saveAsTextFile("Output");
    val outPut = listRDD.flatMap {
      case (label, list) => {
        list.map((label, _))
      }
    }
    println(outPut.foreach(println))
    outPut.saveAsTextFile("Output");
  }
}