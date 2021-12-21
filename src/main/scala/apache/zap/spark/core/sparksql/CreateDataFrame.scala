package apache.zap.spark.core.sparksql

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

/**
  * DataFrame is a Dataset organized into named columns. Conceptually, they are equivalent to a table
  * in a relational database or a DataFrame in R or Python but with richer optimizations under the hood.
  *
  * DataFrame has two main advantages over RDD:
  *   1. Optimized execution plans via Catalyst Optimizer.
  *   2. Custom Memory management via Project Tungsten.
  *
  * With a SparkSession, applications can create DataFrames from an
  *   1. existing RDD :
  *       a. Schema reflectively inferred : rdd.toDF()
  *       b. Schema explicitly specified : spark.createDataFrame(rowRDD, schema)
  *   2. from a Hive table,
  *   3. from Spark data sources : Semi-structured/Structured data sources Spark SQL
  *         can directly create dataFrame.
  *         - JSON
  *         - CSV
  *         - JDBC
  *         - Parquet
  */
object CreateDataFrame extends App {

  import spark.implicits._

  private val peopleResource1 = this.getClass.getResource("/TestData/SparkSQL/people1.csv").toString
  private val peopleResource2 = this.getClass.getResource("/TestData/SparkSQL/people2.csv").toString



  val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
  val rdd1 = spark.sparkContext.parallelize(data)
  val columns = Seq("language","users_count")
  //1. from existing RDD
  val rdd = Seq(
    ("GooGle", "Sundar Pichai"),
    ("MicroSoft", "Satya Nadala"),
    ("Amazon", "Jeff Bezos")
  )

  val rddToDfwithOutColNames = rdd.toDF()

  rddToDfwithOutColNames.printSchema()

//  root
//  |-- _1: string (nullable = true)
//  |-- _2: string (nullable = true)

  rddToDfwithOutColNames.show()

//  +---------+-------------+
//  |       _1|           _2|
//  +---------+-------------+
//  |   GooGle|Sundar Pichai|
//    |MicroSoft| Satya Nadala|
//    |   Amazon|   Jeff Bezos|
//    +---------+-------------+

  val rddToDfwithColNames = rdd.toDF("Company", "CEO")

  rddToDfwithColNames.printSchema()

//  root
//  |-- Company: string (nullable = true)
//  |-- CEO: string (nullable = true)

  rddToDfwithColNames.show()

//  +---------+-------------+
//  |  Company|          CEO|
//  +---------+-------------+
//  |   GooGle|Sundar Pichai|
//    |MicroSoft| Satya Nadala|
//    |   Amazon|   Jeff Bezos|
//    +---------+-------------+


  // Using Spark createDataFrame() from SparkSession.toDF(columns:_*)
  val dfFromRDD2 = spark.createDataFrame(rdd).toDF(columns:_*)
  dfFromRDD2.show(false)

  val someData = Seq(
    Row(8, "bat"),
    Row(64, "mouse"),
    Row(-27, "horse")
  )

  val someSchema = List(
    StructField("number", IntegerType, true),
    StructField("word", StringType, true)
  )

  val someDF = spark.createDataFrame(
    spark.sparkContext.parallelize(someData),
    StructType(someSchema)
  )
  // 2. See later

  // 3. from Spark DataSources
  val df1 = spark
    .read
    .option("header",false)
    .csv(peopleResource1)

  df1.printSchema()

//  root
//  |-- _c0: string (nullable = true)
//  |-- _c1: string (nullable = true)
//  |-- _c2: string (nullable = true)

  df1.show()

//  +-----+---+--------------------+
//  |  _c0|_c1|                 _c2|
//  +-----+---+--------------------+
//  |Jorge| 30|      Java Developer|
//    |  Bob| 32|Full Stack Developer|
//  +-----+---+--------------------+

  val df2 = spark
    .read
    .option("header",true)
    .csv(peopleResource2)

  df2.printSchema()

//  root
//  |-- name: string (nullable = true)
//  |-- age: string (nullable = true)
//  |-- job: string (nullable = true)

  df2.show()

//  +-----+---+--------------------+
//  | name|age|                 job|
//  +-----+---+--------------------+
//  |Jorge| 30|      Java Developer|
//  |  Bob| 32|Full Stack Developer|
//  +-----+---+--------------------+

}
