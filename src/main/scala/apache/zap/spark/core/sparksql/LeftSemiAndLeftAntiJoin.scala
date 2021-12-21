package apache.zap.spark.core.sparksql

/**
  * Left Semi Join and Left anti joins
  *
  * - These are the only joins that only have values from the left table. A left semi join is the same as
  * filtering the left table for only rows with keys present in the right table.
  * - The left anti join also only returns data from the left table, but instead only returns records that
  * are not present in the right table.
  *
  * -  .joinWith API don't support these joins. Default `inner`. Must be one of:
  *                 `inner`, `cross`, `outer`, `full`, `full_outer`, `left`, `left_outer`,
  *                 `right`, `right_outer`.
  *
  *
  */
object LeftSemiAndLeftAntiJoin extends App {

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  case class Customer(customer_id: Int, customer_name: String)
  case class Payment(payment_id: Int, customer_id: Int, amount: Int)
  case class ID(amount: Int, customer_name: String)

  case class InnerJoinedRows[A, B](left: A, right: B)

  val payment = sc.parallelize(Seq(
    (1, 101,2500), (2,102,1110), (3,103,500), (4 ,104,400), (5 ,105, 150), (6 ,106, 450)
  )).toDS().toDF("payment_id", "customer_id", "amount").as[Payment]

  payment.show()

  //  +---------+----------+------+
  //  |paymentId|customerId|amount|
  //  +---------+----------+------+
  //  |        1|       101|  2500|
  //  |        2|       102|  1110|
  //  |        3|       103|   500|
  //  |        4|       104|   400|
  //  |        5|       105|   150|
  //  |        6|       106|   450|
  //  +---------+----------+------+

  val customer = sc.parallelize(Seq((101,"Jon") , (102,"Aron") ,(103,"Sam")))
    .toDF("customer_id", "customer_name").as[Customer]

  customer.show()

  //  +----------+----+
  //  |customerId|name|
  //  +----------+----+
  //  |       101| Jon|
  //  |       102|Aron|
  //  |       103| Sam|
  //  +----------+----+

  // Left Semi Join
  val leftSemiJoinResult1 = payment
    .join(customer, Seq("customer_id"), "left_semi")

  leftSemiJoinResult1.show()

//  +-----------+----------+------+
//  |customer_id|payment_id|amount|
//  +-----------+----------+------+
//  |        101|         1|  2500|
//  |        103|         3|   500|
//  |        102|         2|  1110|
//  +-----------+----------+------+

  val leftSemiJoinResult2 = payment.as("p")
    .join(customer.as("c"), $"p.customer_id" === $"c.customer_id", "left_semi")

  leftSemiJoinResult2.show()

//  +----------+-----------+------+
//  |payment_id|customer_id|amount|
//  +----------+-----------+------+
//  |         1|        101|  2500|
//  |         3|        103|   500|
//  |         2|        102|  1110|
//  +----------+-----------+------+

  // Left Anti Join
  val leftAntiJoinResult1 = payment
    .join(customer, Seq("customer_id"), "left_anti")

  leftAntiJoinResult1.show()

//  +-----------+----------+------+
//  |customer_id|payment_id|amount|
//  +-----------+----------+------+
//  |        105|         5|   150|
//  |        106|         6|   450|
//  |        104|         4|   400|
//  +-----------+----------+------+

  val leftAntiJoinResult2 = payment.as("p")
    .join(customer.as("c"), $"p.customer_id" === $"c.customer_id", "left_anti")

  leftAntiJoinResult2.show()

  //  +-----------+----------+------+
  //  |customer_id|payment_id|amount|
  //  +-----------+----------+------+
  //  |        105|         5|   150|
  //  |        106|         6|   450|
  //  |        104|         4|   400|
  //  +-----------+----------+------+


}
