package apache.zap.spark.core.sparksql

import org.apache.spark.sql.SparkSession
import java.sql.Timestamp

object InferSchema extends App {

  val inferSchemaJsonfile = this.getClass.getResource("/TestData/SparkSQL/inferSchema.json").toString

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("InferSchema")
    .getOrCreate()

  val df = spark
    .read.json(inferSchemaJsonfile)

  df.printSchema()
  df.show(false)


  // using StructType
  println("---------- Using StructType -------------")
  import org.apache.spark.sql.types._
  val structTypeSchema = StructType(
    StructField("title", StringType, true) ::
      StructField("ticket_total_value", IntegerType, true) ::
      StructField("sale_ts", TimestampType, true) :: Nil)

  val df1 = spark
    .read
    .schema(structTypeSchema)
    .json(inferSchemaJsonfile)

  df1.printSchema()
  df1.show(false)

  //using Encoder
  println("---------- Using Encoder -------------")
  import org.apache.spark.sql.Encoders
  case class Json(title: String, ticket_total_value: Int, sale_ts: Timestamp)
  val encoderSchema = Encoders.product[Json].schema

  val df2 = spark
    .read
    .schema(encoderSchema)
    .json(inferSchemaJsonfile)

  df2.printSchema()
  df2.show(false)

  //using case class explicitly
  println("---------- Using case class explicitly -------------")
  import spark.implicits._
  val df3 = spark
    .read
    .json(inferSchemaJsonfile)
    .map(row => Json(row.getString(0), row.getString(1).toInt, convertString2Timestamp(row.getString(2))))


  private def convertString2Timestamp(str: String): Timestamp = {
    if (str.isEmpty) {
      null
    } else {
      new Timestamp(str.toLong)
    }
  }
}
