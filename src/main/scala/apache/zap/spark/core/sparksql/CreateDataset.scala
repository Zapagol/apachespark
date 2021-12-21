package apache.zap.spark.core.sparksql

import org.apache.spark.sql.Row
/**
  * Dataset provides the benefits of RDDs (strong typing, ability to use powerful lambda functions)
  * with the benefits of Spark SQL’s optimized execution engine.
  *
  *   Dataset = RDD(Strogly typed) + Spark SQL's Optimizations
  *
  * We can create Dataset in many ways
  *1. Using toDS() :
  *     a. from an RDD
  *     b. from common Scala Types
  * 2. Using createDataset() :
  *     Creates a [[org.apache.spark.sql.Dataset]]from a local Seq/List/RDD of data of a given type. This
  *     method requires an encoder (to convert a JVM object of type `T` to and from the internal Spark
  *     SQL representation) that is generally created automatically through implicits from a `SparkSession`,
  *     or can be created explicitly by calling static methods on [[org.apache.spark.sql.Encoders]].
  *
  * 3. From DataFrame
  */
object CreateDataset extends App {

  import spark.implicits._


  case class Employee(name: String, company: String, salary: Int)

  // 1. Using toDS()
  // from common Scala Types
  val seqIntDS = Seq(1,2,3,4).toDS()
  val listIntDS = List(1,2,3,4).toDS()
  val mapDS = Map((1,"One"),(2,"Two")).toList.toDS()
  val arrayDS = Array(1,2,3).toSeq.toDS()
  val seqCaseClassDS = Seq(
    Employee("Sundar Pichai", "Google", 650000),
    Employee("Satya Nadella", "MicroSoft", 18300000),
    Employee("Jeff Bezoz", "Amazon", 81840000)
  ).toDS()

  // from an RDD
  val rddIntDS = sc.parallelize(Seq(1,2,3,4)).toDS()
  val kayValueRDD = sc.parallelize(Seq((1, "Spark"), (2, "Databricks"), (3, "Notebook"))).toDS()

  // 2. Using createDataset

  //using util.List[T]
  val listDataset = spark.createDataset(List(1,2,3))
  //using Seq[T]
  val seqDataset = spark.createDataset(Seq(1,2,3))
  //using RDD[T]
  val rddDataset = spark.createDataset(sc.parallelize(Seq(1,2,3)))

  // 3. From DataFrame

  //using .as[T] => by implicit conversions
  case class Company(companyName: String, founded: Int, founder: String, numOfEmployees: Int)
  val companySeq = Seq(Company("SparkSQL", 1998, "Larry Page and Sergey Brin", 98800),
    Company("MicroSoft", 1975, "Bill Gates and Paul Allen", 13500),
    Company("SparkSQL", 1994, "Jeff Bezos",647500))
  val companyDF = sc.parallelize(companySeq).toDF()

  val companyDS = companyDF.as[Company]
  companyDS.show(false)
//  +-----------+-------+--------------------------+--------------+
//  |companyName|founded|founder                   |numOfEmployees|
//  +-----------+-------+--------------------------+--------------+
//  |SparkSQL      |1998   |Larry Page and Sergey Brin|98800         |
//  |MicroSoft     |1975   |Bill Gates and Paul Allen |13500         |
//  |SparkSQL      |1994   |Jeff Bezos                |647500        |
//  +-----------+-------+--------------------------+--------------+


  /** */
  val sparkComponentsRDD = sc.parallelize(Seq((1, "Spark Core"),
    (2, "Spark"),
    (3, "Spark Streaming"),
    (4, "MLib"),
    (5, "GraphX")))
  val sparkComponentsDF = sparkComponentsRDD.toDF("Id", "SparkComponentName")

  val sparkComponentsDS = sparkComponentsDF.as[(Int, String)]
  val list = sparkComponentsDS.select("SparkComponentName").collect().map(_(0)).toList
  sparkComponentsDS.show(false)

//  +---+------------------+
//  |Id |SparkComponentName|
//  +---+------------------+
//  |1  |Spark Core        |
//  |2  |Spark SQL         |
//  |3  |Spark Streaming   |
//  |4  |MLib              |
//  |5  |GraphX            |
//  +---+------------------+

  // using .map()

  val companyDSUsingMap = companyDF.map( row => fromRow(row))
  companyDSUsingMap.show(false)

  val map = companyDSUsingMap
    .map(x => Map(x.companyName -> x.founded))

//  +-----------+-------+--------------------------+--------------+
//  |companyName|founded|founder                   |numOfEmployees|
//  +-----------+-------+--------------------------+--------------+
//  |Google     |1998   |Larry Page and Sergey Brin|98800         |
//  |MicroSoft  |1975   |Bill Gates and Paul Allen |13500         |
//  |Amazon     |1994   |Jeff Bezos                |647500        |
//  +-----------+-------+--------------------------+--------------+

  def fromRow(row: Row): Company = {
    Company(
      companyName = row.getString(0),
      founded = row.getInt(1),
      founder = row.getString(2),
      numOfEmployees = row.getInt(3)
    )
  }

}
