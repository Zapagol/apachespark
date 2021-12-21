package apache.zap.spark.core.sparksql

import org.apache.spark.sql.functions.udf
/**
  * User-Defined Functions (UDFs) are user-programmable routines that act on one row.
  *
  *   - UDF’s are used to extend the functions of the framework and re-use this function on several
  *   DataFrame. For example if you wanted to convert the every first letter of a word in a sentence
  *   to capital case, spark build-in features does’t have this function hence you can create it as UDF
  *   and reuse this as needed on many Data Frames. UDF’s are once created they can be re-use on several
  *   DataFrame’s and SQL expressions.
  */
object UDF extends App {

  // Define and register a zero-argument non-deterministic UDF
  // UDF is deterministic by default, i.e. produces the same result for the same input.
  val random = udf(() => Math.random())
  spark.udf.register("random", random.asNondeterministic())
  spark.sql("SELECT random()").show()
  // +-------+
  // |UDF()  |
  // +-------+
  // |xxxxxxx|
  // +-------+

  val plusOne = udf((x: Int)=> x + 1)
  spark.udf.register("plusOne", plusOne)
  spark.sql("select plusOne(10)").show(false)
//  +-------+
//  |UDF(10)|
//  +-------+
//  |11     |
//  +-------+

  //UDF in where clause
  val evenFilterUDF = udf((x: Int) => x % 2 == 0)
  spark.udf.register("evenFilterUDF", evenFilterUDF)
  spark.range(1, 20).createOrReplaceTempView("test")
  spark.sql("select * from test where evenFilterUDF(id)").show(false)

//  +---+
//  |id |
//  +---+
//  |2  |
//  |4  |
//  |6  |
//  |8  |
//  |10 |
//  |12 |
//  |14 |
//  |16 |
//  |18 |
//  +---+

  val squared = (x: Int) => x * x
  spark.udf.register("squared", squared)
  spark.sql("select id, squared(id) as id_squared from test").show(false)

//  +---+----------+
//  |id |id_squared|
//  +---+----------+
//  |1  |1         |
//  |2  |4         |
//  |3  |9         |
//  |4  |16        |
//  |5  |25        |
//  |6  |36        |
//  |7  |49        |
//  |8  |64        |
//  |9  |81        |
//  |10 |100       |
//  |11 |121       |
//  |12 |144       |
//  |13 |169       |
//  |14 |196       |
//  |15 |225       |
//  |16 |256       |
//  |17 |289       |
//  |18 |324       |
//  |19 |361       |
//  +---+----------+

  // With DataFrame
  import org.apache.spark.sql.functions.{col, udf}
  val squared1 = udf((s: Long) => s * s)
  spark.range(1, 10).select(squared1(col("id")) as "id_squared").show(false)

}
