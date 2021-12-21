package apache.zap.spark.core.sparksql.datasources

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

object HiveWithBucketing extends App {

  import spark.implicits._

  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
  case class Customer(customer_id: Int, customer_name: String)
  case class Payment(payment_id: Int, customer_id: Int, amount: Int)
  case class ID(amount: Int, customer_name: String)

  case class InnerJoinedRows[A, B](left: A, right: B)

  val payment = sc.parallelize(Seq(
    (1, 101,2500), (2,102,1110), (3,103,500), (4 ,104,400), (5 ,105, 150), (6 ,106, 450)
  )).toDF("payment_id", "customer_id", "amount").as[Payment]

  val customer = sc.parallelize(Seq((101,"Jon") , (102,"Aron") ,(103,"Sam")))
    .toDF("customer_id", "customer_name").as[Customer]


  payment.write
    .bucketBy(4, "customer_id")
    //.sortBy("customer_id")
    .mode(SaveMode.Overwrite)
    .saveAsTable("bucketed_payment")

  customer.write
    .bucketBy(4, "customer_id")
    //.sortBy("customer_id")
    .mode(SaveMode.Overwrite)
    .saveAsTable("bucketed_customer")

  val bucketed_payment = spark.table("bucketed_payment")
  val bucketed_customer = spark.table("bucketed_customer")

  //val joined = bucketed_payment.join(bucketed_customer, "customer_id")
  val joined = bucketed_payment.as("p")
    .join(bucketed_customer.as("c"), $"p.customer_id" === $"c.customer_id", "left_outer")

  val bridged = joined.filter($"c.customer_id".isNotNull)
  val unbridged = joined.filter($"c.customer_id".isNull)

  joined.show(false)
  bridged.show(false)
  unbridged.show(false)
  bridged.unpersist()

//  val df1 = joined.select("payment_id", "customer_id", "amount")
//  val df2 = joined.select("customer_id", "customer_name")

//  df1.show(false)
//  df2.show(false)

  Thread.sleep(10000000)
}
