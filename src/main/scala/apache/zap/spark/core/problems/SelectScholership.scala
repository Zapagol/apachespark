package apache.zap.spark.core.problems

import apache.zap.spark.core.converters.Demographic
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object SelectScholership extends App{

  val logger = Logger.getLogger(FriendsByAvgAge.getClass)

  private val demoGraphicResource = this.getClass.getResource("/TestData/demographic.txt").toString
  private val financeResource = this.getClass.getResource("/TestData/finances.txt").toString

  val spark = SparkSession
    .builder()
    .appName("Select Scholership")
    //.config("spark.eventLog.enabled", "true")
    .master("local")
    .getOrCreate()

  val dempoGraphicRDD = spark.sparkContext.textFile(demoGraphicResource)
  //val financeRDD = spark.sparkContext.textFile(financeResource).map(Demographic.fromCsvRow())

  //Possibility - 1
  //val eligibleSchlershipEmpCount =

}
