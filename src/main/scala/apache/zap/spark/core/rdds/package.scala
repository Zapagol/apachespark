package apache.zap.spark.core

import org.apache.spark.sql.SparkSession

package object rdds {

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("Spark RDD's")
    .getOrCreate()

  val sc = spark.sparkContext

}
