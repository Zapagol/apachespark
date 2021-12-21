package apache.zap.spark.core

import org.apache.spark.sql.SparkSession

package object sparksql {

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("Spark SQL")
    .getOrCreate()

  val sc = spark.sparkContext

}
