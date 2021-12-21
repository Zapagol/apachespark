package apache.zap.spark.core.problems

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object FriendsByAvgAge extends App {

  val logger = Logger.getLogger(FriendsByAvgAge.getClass)

  private val friendsResource = this.getClass.getResource("/TestData/fakefriends.csv").toString

  val spark = SparkSession
    .builder()
    .appName("Friends By Average Age")
    //.config("spark.eventLog.enabled", "true")
    .master("local")
    .getOrCreate()

  val lines = spark.sparkContext.textFile(friendsResource)

  val rdd = lines.map{
    line => val fields = line.split(",")
      val age = fields(2).toInt
      val numFriends = fields(3).toInt
      (age,numFriends)
  }

  val ageByFriends = rdd.mapValues( x => (x,1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))

  val avergeByAge = ageByFriends.mapValues( x => x._1/x._2)

  // Collect the results from the RDD (This kicks off computing the DAG and actually executes the job)
  val results = avergeByAge.collect()

  // Sort and print the final results.
  results.sorted.foreach(println)
}
