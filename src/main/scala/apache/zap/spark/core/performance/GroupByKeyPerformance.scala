package apache.zap.spark.core.performance

import java.sql.Timestamp
import java.time.LocalDateTime

import org.apache.spark.sql.SparkSession

object GroupByKeyPerformance {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("GroupByKey Example")
      .getOrCreate()

    val start_time = System.nanoTime()

    val fileRdd =
      spark.sparkContext.textFile("C:\\Users\\KZAPAGOL\\Desktop\\README.md")

    val wordCountRdd = fileRdd
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .groupByKey()
      .map(t => (t._1, t._2.sum))
      .collect()

    val end_time = System.nanoTime()

    val elapsedTimeInSecond =  (end_time - start_time) / 1000000000
    println("Elapsed time in seconds = " + elapsedTimeInSecond)

  }
}
