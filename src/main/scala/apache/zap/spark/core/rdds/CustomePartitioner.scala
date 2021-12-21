package apache.zap.spark.core.rdds

import apache.zap.spark.core.models.{CustomePartitioner, Event}

/**
  * Custom Partitioning: Spark allows users to create custom partitioners by extending the
  *      default Partitioner class. So that we can specify the data to be stored in each partition.
  *
  */
object CustomePartitioner extends App {

  private val eventResource =
    this.getClass.getResource("/TestData/event.txt").toString

  import spark.implicits._

  val eventDS = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",")
    .csv(eventResource)
    .as[Event]

  val eventRDD = eventDS
    .map(event => (event.organizer, event.budget))
    .rdd
    .partitionBy(new CustomePartitioner(3))

  eventRDD
    .reduceByKey(_ + _)
    .saveAsTextFile("C:/Documents/Kiran/Spark/CustomePartitioner")

}
