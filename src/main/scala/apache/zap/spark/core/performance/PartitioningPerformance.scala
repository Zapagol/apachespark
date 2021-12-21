package apache.zap.spark.core.performance

import org.apache.spark.sql.SparkSession
import org.apache.spark._

object PartitioningPerformance extends App {

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("Partitioning Performance Example")
    .getOrCreate()

  val start_time = System.nanoTime()

  val fileRdd =
    spark.sparkContext.textFile("C:/Users/KZAPAGOL/Desktop/README.md",4)

  val words = fileRdd
    .flatMap(line => line.split(" "))
    .map(word => (word, 1))

  val wordsWithPartitioner = words
    .partitionBy(new HashPartitioner(4))

  val wordCount = wordsWithPartitioner
    .reduceByKey(_ + _)
    .collect()

  val end_time = System.nanoTime()
  val elapsedTimeInSecond = (end_time - start_time) / 1000000000

  println("Elapsed time in seconds = " + elapsedTimeInSecond)

}
