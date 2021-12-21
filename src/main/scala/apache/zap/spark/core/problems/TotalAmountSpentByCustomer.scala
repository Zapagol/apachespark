package apache.zap.spark.core.problems

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions._

object TotalAmountSpentByCustomer {
  import spark.implicits._
  private val customerOrders = this.getClass.getResource("/TestData/customer-orders.csv").toString
  case class Customer(customer_id: Int, product_id: Int, amount: Double)

  def main(args: Array[String]): Unit ={
    toatlAmountDS
  }

  def toatlAmountDF(): Unit ={
    val schema = StructType(
      StructField("customer_id", IntegerType, true) ::
        StructField("product_id", IntegerType, true) ::
        StructField("amount", DoubleType, true) :: Nil)

    val customerDF = spark
      .read
      .schema(schema)
      .option("header", "false")
      .option("inferSchema", "false")
      .option("sep", ",")
      .csv(customerOrders)

    customerDF
      .groupBy("customer_id")
      .sum("amount").as("total_amount")
      .show(false)
  }

  def toatlAmountDS(): Unit ={
    import org.apache.spark.sql.Encoders
    val encoderSchema =Encoders.product[Customer].schema

    val customerDS = spark
      .read
      .schema(encoderSchema)
      .csv(customerOrders)

    customerDS
      .groupBy($"customer_id")
      .agg(sum("amount") as "total_sum")
      .sort("total_sum")
      .show(false)
  }

}
