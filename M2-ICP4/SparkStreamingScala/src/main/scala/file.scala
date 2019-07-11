import scala.io.{Source}
import java.io.{File, PrintWriter}


object file {

  def main(args: Array[String]):Unit = {
    var a = 1

    while (a <=100) {

      //Data to write in File using PrintWriter

      val writer = new PrintWriter(new File("/Users/anushakonakalla/Desktop/logs/log" +a+ ".txt"))


      val filename = "/Users/anushakonakalla/Desktop/lorem.txt"
      for (line <- Source.fromFile(filename).getLines) {

        writer.write(line)
        writer.write("\n")


      }
      Thread.sleep(5000)
      a = a+ 1
      writer.close()

    }
  }
}