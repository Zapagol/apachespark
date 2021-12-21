package apache.zap.spark.core.problems

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object ECommerce {
  private val productSource = this.getClass.getResource("/TestData/Ecommenrce/product.csv").toString
  private val salesSource = this.getClass.getResource("/TestData/Ecommenrce/sales.csv").toString
  private val sellerSource = this.getClass.getResource("/TestData/Ecommenrce/sellers.csv").toString

  def main(args: Array[String]): Unit = {
    val productDF = spark
      .read
      .option("inferSchema", true)
      .option("header", true)
      .option("delimeter", ",")
      .csv(productSource)

    val salesDF = spark
      .read
      .option("header", true)
      .option("inferSchema", true)
      .option("delimeter", ",")
      .csv(salesSource)

    val sellerDF = spark
      .read
      .option("header", true)
      .option("inferschema", true)
      .option("delimeter", ",")
      .csv(sellerSource)

    howManyOrdersSellersProducts(productDF, salesDF, sellerDF)
    productSoldAtLeastOnce(salesDF)(spark)
  }
  def howManyOrdersSellersProducts(productDF: DataFrame, salesDF: DataFrame, sellersDF: DataFrame): Unit ={
    println("Number of products = " + productDF.count)
    println("Number of orders = " + salesDF.count)
    println("Number of sellers = " + sellersDF.count)
  }
  def productSoldAtLeastOnce(salesDF: DataFrame)(implicit spark: SparkSession): Unit ={
    import spark.implicits._
    salesDF
      .groupBy("product_id")
      .agg(count("*").alias("count"))
      .filter($"count" > 1)
      .count()
  }
  def productWithMoreOrders(salesDF: DataFrame)(implicit spark: SparkSession): Unit ={
    import spark.implicits._
    salesDF
      .groupBy("product_id")
      .agg(countDistinct("*").alias("count"))
      .orderBy(col("cnt").desc)
      .limit(1)
      .show()
  }
  def distinctProductEachDay(salesDF: DataFrame)(implicit sparkSession: SparkSession): Unit ={
    salesDF
      .groupBy("date")
      .agg(countDistinct("product_id").alias("count"))
      .show()
  }
}
