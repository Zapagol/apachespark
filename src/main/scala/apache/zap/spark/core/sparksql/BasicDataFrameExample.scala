package apache.zap.spark.core.sparksql

import org.apache.spark.sql.functions._
/**
  * We can select and work with columns in 3 ways.
  *   1. Using $-notation
  *   2. Referring to DF
  *   3. Using SQL query string
  *   4. Using `col` spark sql function
  *
  * SparkSession enables applications to run SQL queries programmatically
  * and returns the result as a DataFrame.Before we should create temporary/global view.
  *   1. createOrReplaceTempView : Creates a local temporary view using the given name
  *     and these are session scoped and will disappear if the session that creates it terminates.
  *
  *   2. createOrReplaceGlobalTempView : temporary view that is shared among all sessions and
  *     keep alive until the Spark application terminates. Global temporary view is tied to a
  *     system preserved database global_temp.
  */
object BasicDataFrameExample extends App {

  import spark.implicits._

  val capitalDF = Seq(
    ("India", "New Delhi",16900000),
    ("Germany", "Berlin",4000000),
    ("Japan", "Tokyo",13900000),
    ("UK", "London",1200000),
    ("Spain", "Madrid",3850000),
    ("AUSTRALIA","",550000)
  ).toDF("country","capital","population")

  capitalDF.show()

//  +---------+---------+----------+
//  |  country|  capital|population|
//  +---------+---------+----------+
//  |    India|New Delhi|  16900000|
//    |  Germany|   Berlin|   4000000|
//  |    Japan|    Tokyo|  13900000|
//  |       UK|   London|   1200000|
//  |    Spain|   Madrid|   3850000|
//  |AUSTRALIA|         |    550000|
//    +---------+---------+----------+

  capitalDF.printSchema()

//  root
//  |-- country: string (nullable = true)
//  |-- capital: string (nullable = true)
//  |-- population: integer (nullable = false)

  //1. Using $-notation
  capitalDF.filter($"population" > 8000000).show()

//  +-------+---------+----------+
//  |country|  capital|population|
//  +-------+---------+----------+
//  |  India|New Delhi|  16900000|
//  |  Japan|    Tokyo|  13900000|
//  +-------+---------+----------+

  //2. Referring to the dataFrame
  capitalDF.filter(capitalDF("population") > 8000000).show()

//  +-------+---------+----------+
//  |country|  capital|population|
//  +-------+---------+----------+
//  |  India|New Delhi|  16900000|
//  |  Japan|    Tokyo|  13900000|
//  +-------+---------+----------+

  //3. Using SQL query String
  capitalDF.filter("population > 8000000").show()

//  +-------+---------+----------+
//  |country|  capital|population|
//  +-------+---------+----------+
//  |  India|New Delhi|  16900000|
//  |  Japan|    Tokyo|  13900000|
//  +-------+---------+----------+

  //4. Using `col` spark sql function
  capitalDF.filter(col("population") > 8000000).show()

  //  +-------+---------+----------+
  //  |country|  capital|population|
  //  +-------+---------+----------+
  //  |  India|New Delhi|  16900000|
  //  |  Japan|    Tokyo|  13900000|
  //  +-------+---------+----------+
  //Temporary view
  capitalDF.createOrReplaceTempView("nationalTempView")

  spark.sql("select * from nationalTempView").show()

//  +---------+---------+----------+
//  |  country|  capital|population|
//  +---------+---------+----------+
//  |    India|New Delhi|  16900000|
//    |  Germany|   Berlin|   4000000|
//  |    Japan|    Tokyo|  13900000|
//  |       UK|   London|   1200000|
//  |    Spain|   Madrid|   3850000|
//  |AUSTRALIA|         |    550000|
//    +---------+---------+----------+

  spark.sql("select * from nationalTempView where population > 8000000").show()

//  +-------+---------+----------+
//  |country|  capital|population|
//  +-------+---------+----------+
//  |  India|New Delhi|  16900000|
//    |  Japan|    Tokyo|  13900000|
//  +-------+---------+----------+

  //Global view
  capitalDF.createOrReplaceGlobalTempView("nationalGlobalView")

  spark.sql("select * from global_temp.nationalGlobalView").show()

  //  +---------+---------+----------+
  //  |  country|  capital|population|
  //  +---------+---------+----------+
  //  |    India|New Delhi|  16900000|
  //    |  Germany|   Berlin|   4000000|
  //  |    Japan|    Tokyo|  13900000|
  //  |       UK|   London|   1200000|
  //  |    Spain|   Madrid|   3850000|
  //  |AUSTRALIA|         |    550000|
  //    +---------+---------+----------+

  spark.sql("select * from global_temp.nationalGlobalView where population > 8000000").show()

  //  +-------+---------+----------+
  //  |country|  capital|population|
  //  +-------+---------+----------+
  //  |  India|New Delhi|  16900000|
  //    |  Japan|    Tokyo|  13900000|
  //  +-------+---------+----------+


}
