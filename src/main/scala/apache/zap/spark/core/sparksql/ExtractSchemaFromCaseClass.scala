package apache.zap.spark.core.sparksql

object ExtractSchemaFromCaseClass extends App {

  import spark.implicits._

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

  // we assume column names in the database are upper case
  val schema = companyDS.toDF(companyDS.columns.map(_.toUpperCase): _*).schema
  println(schema)

//  StructType(StructField(COMPANYNAME,StringType,true), StructField(FOUNDED,IntegerType,false),
//    StructField(FOUNDER,StringType,true), StructField(NUMOFEMPLOYEES,IntegerType,false))

}
