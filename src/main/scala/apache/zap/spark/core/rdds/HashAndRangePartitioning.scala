package apache.zap.spark.core.rdds

import org.apache.spark._

/**
  * The total number of partitions are configurable, by default it is set to the total number
  * of cores on all the executor nodes.
  *
  * Hash Partitioning : Uses Java’s Object.hashCode method to determine the partition as
  *           partition = key.hashCode() % numPartitions.It is the default partitioner of
  *           Spark.
  *
  * Range Partitioning : Uses a range to distribute to the respective partitions the keys
  *           that fall within a range. This method is suitable where there’s a natural ordering
  *           in the keys and the keys are non negative.
  */
object HashAndRangePartitioning extends App {

  private val readMeSource =
    this.getClass.getResource("/TestData/README.md").toString

  val file = sc.textFile(readMeSource)

  val words = file
    .flatMap(line => line.split(" "))
    .map(word => (word, 1))

  //HashPartitioner
  val wordWithHP = words
    .partitionBy(new HashPartitioner(6))
  wordWithHP
    .reduceByKey(_ + _)
    .saveAsTextFile("C:/Documents/Kiran/Spark/HashPartitioner")

  //RangePartitioner
  val wordWithRP = words
    .partitionBy(new RangePartitioner(6, words))
  wordWithHP
    .reduceByKey(_ + _)
    .saveAsTextFile("C:/Documents/Kiran/Spark/RangePartitioner")

}
