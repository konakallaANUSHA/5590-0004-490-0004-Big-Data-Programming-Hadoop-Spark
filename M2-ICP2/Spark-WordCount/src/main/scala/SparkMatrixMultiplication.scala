import org.apache.spark.{SparkConf, SparkContext}

object SparkMatrixMultiplication {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","C:\\Winutils");

    val sparkConf = new SparkConf().setAppName("SparkMatrixMultiplication").setMaster("local[*]")

    val sc=new SparkContext(sparkConf)

    val A = Array(Array(1, 2), Array(3, 4))
    val B = Array(Array(-3, -8, 3), Array(-2, 1, 4))

    def mult[A](a: Array[Array[A]], b: Array[Array[A]])(implicit n: Numeric[A]) = {
      import n._
      for (row <- a)
        yield for(col <- b.transpose)
          yield row zip col map Function.tupled(_*_) reduceLeft (_+_)
    }

    val C = mult(A, B)

    Console.println("C matrix :" )
    C.foreach(println)




  }
}
