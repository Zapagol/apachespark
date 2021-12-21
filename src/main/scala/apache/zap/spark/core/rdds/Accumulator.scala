package apache.zap.spark.core.rdds

import apache.zap.spark.core.problems.FriendsByAvgAge
import org.apache.log4j.Logger

object Accumulator extends App {

  val logger = Logger.getLogger(FriendsByAvgAge.getClass)

  private val logFile =
    this.getClass.getResource("/TestData/apacheLogFile.txt").toString

  val blankLines = sc.longAccumulator("Blank Lines")
  val mozillaUsers = sc.longAccumulator("Mozilla Users")
  val chromeUsers = sc.longAccumulator("Chrome Users")
  val errorCode500 = sc.longAccumulator("500 error")

  val logFileRdd = spark.sparkContext.textFile(logFile)

  logFileRdd.foreach { line =>
    {
      if (line.isEmpty) blankLines.add(1L)
      else {

        val fields = line.split(" ")
        val website = fields(9)
        val code = fields(6)

        if (website.equals("Mozilla")) mozillaUsers.add(1L)
        if (website.equals("Chrome")) chromeUsers.add(1L)
        if (code.equals("500")) errorCode500.add(1L)
      }
    }
  }

  println(s"\tBlank Lines = ${blankLines.value}")
  println(s"\tNumber of Mozilla Users = ${mozillaUsers.value}")
  Thread.sleep(1000000)
  println(s"\tNumber of Chrome Users = ${chromeUsers.value}")
  println(s"\tNumber of Error response =${errorCode500.value}")
}
