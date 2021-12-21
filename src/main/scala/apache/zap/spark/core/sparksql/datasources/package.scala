package apache.zap.spark.core.sparksql

import org.apache.spark.sql.SparkSession

package object datasources {

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("Spark SQL")
    .getOrCreate()

  val sc = spark.sparkContext
}
