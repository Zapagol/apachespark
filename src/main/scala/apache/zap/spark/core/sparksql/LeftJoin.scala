package apache.zap.spark.core.sparksql

import org.apache.spark.sql.functions._

object LeftJoin extends App {

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  case class Customer(customerId: Int, customer_name: String)
  case class Payment(paymentId: Int, customerId: Int, amount: Int)
  case class LeftJoinedRows[A, B](left: A, right: Option[B])

  val payment = sc.parallelize(Seq(
    (1, 101,2500), (2,102,1110), (3,103,500), (4 ,104,400), (5 ,105, 150), (6 ,106, 450)
  )).toDF("paymentId", "customerId","amount").as[Payment]
    Seq("", "")
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

  val customer = sc.parallelize(Seq((101,"Jon") , (102,"Aron") ,(103,"Sam"), (120, "Robert")))
    .toDF("customerId", "customer_name").as[Customer]

  customer.show()

  //  +----------+----+
  //  |customerId|name|
  //  +----------+----+
  //  |       101| Jon|
  //  |       102|Aron|
  //  |       103| Sam|
  //  +----------+----+

  val leftJoinDF1 = payment
    .join(customer, Seq("customerId"), "left")

  leftJoinDF1.show()

//  +----------+---------+------+----+
//  |customerId|paymentId|amount|name|
//  +----------+---------+------+----+
//  |       101|        1|  2500| Jon|
//  |       103|        3|   500| Sam|
//  |       102|        2|  1110|Aron|
//  |       105|        5|   150|null|
//  |       106|        6|   450|null|
//  |       104|        4|   400|null|
//  +----------+---------+------+----+

  val leftJoinDF2 = payment
    .join(customer, Seq("customerId"), "left_outer")

  leftJoinDF2.show()

  //  +----------+---------+------+----+
  //  |customerId|paymentId|amount|customer_name|
  //  +----------+---------+------+----+
  //  |       101|        1|  2500| Jon|
  //  |       103|        3|   500| Sam|
  //  |       102|        2|  1110|Aron|
  //  |       105|        5|   150|null|
  //  |       106|        6|   450|null|
  //  |       104|        4|   400|null|
  //  +----------+---------+------+----+
  val bridgedDF2 = leftJoinDF2
    .filter(col("customer_name").isNotNull)
    .show(false)

//  +----------+---------+------+-------------+
//  |customerId|paymentId|amount|customer_name|
//  +----------+---------+------+-------------+
//  |101       |1        |2500  |Jon          |
//  |103       |3        |500   |Sam          |
//  |102       |2        |1110  |Aron         |
//  +----------+---------+------+-------------+

  val unbridgedDF2 = leftJoinDF2
    .filter(col("customer_name").isNull)
    .show(false)

//  +----------+---------+------+-------------+
//  |customerId|paymentId|amount|customer_name|
//  +----------+---------+------+-------------+
//  |105       |5        |150   |null         |
//  |106       |6        |450   |null         |
//  |104       |4        |400   |null         |
//  +----------+---------+------+-------------+

  val leftJoinDF3 = payment
      .as("p")
      .join(customer.as("c"), $"p.customerId" === $"c.customerId", "left_outer")

  leftJoinDF3.show()

//  +---------+----------+------+----------+----+
//  |paymentId|customerId|amount|customerId|name|
//  +---------+----------+------+----------+----+
//  |        1|       101|  2500|       101| Jon|
//  |        3|       103|   500|       103| Sam|
//  |        2|       102|  1110|       102|Aron|
//  |        5|       105|   150|      null|null|
//  |        6|       106|   450|      null|null|
//  |        4|       104|   400|      null|null|
//  +---------+----------+------+----------+----+

  val leftJoinDF4 = payment
    .as("p")
    .joinWith(customer.as("c"), $"p.customerId" === $"c.customerId", "left_outer")

  leftJoinDF4.show()

//  +------------+----------+
//  |          _1|        _2|
//  +------------+----------+
//  |[1,101,2500]| [101,Jon]|
//  | [3,103,500]| [103,Sam]|
//  |[2,102,1110]|[102,Aron]|
//  | [5,105,150]|      null|
//  | [6,106,450]|      null|
//  | [4,104,400]|      null|
//  +------------+----------+


  val leftOuterJoinDF1 = payment.as("p")
    .joinWith(customer.as("c"),
      $"p.customerId" === $"c.customerId","left_outer")
    .map{
      case(left, right) =>LeftJoinedRows(left, Option(right))
    }

  val bridged = leftOuterJoinDF1
    .filter(_.right.isDefined)
    .map(x => (x.left, x.right.get))

  val bridgedRight = leftOuterJoinDF1
    .filter(_.right.isDefined)
    .map(x => x.right.get)

  bridged.show(false)



//  +------------+----------+
//  |_1          |_2        |
//  +------------+----------+
//  |[1,101,2500]|[101,Jon] |
//  |[3,103,500] |[103,Sam] |
//  |[2,102,1110]|[102,Aron]|
//  +------------+----------+

  val bridgedCustomer = bridged.map{
    case(_, customer: Customer) => customer
  }
  bridgedCustomer.show(false)

//  +----------+-------------+
//  |customerId|customer_name|
//  +----------+-------------+
//  |101       |Jon          |
//  |103       |Sam          |
//  |102       |Aron         |
//  +----------+-------------+

  val bridgedPayment = bridged.map{
    case(payment: Payment, _) => payment
  }
  bridgedPayment.show(false)

//  +---------+----------+------+
//  |paymentId|customerId|amount|
//  +---------+----------+------+
//  |1        |101       |2500  |
//  |3        |103       |500   |
//  |2        |102       |1110  |
//  +---------+----------+------+

  val unbridged = leftOuterJoinDF1
    .filter(_.right.isEmpty)
    .map(_.left)

  unbridged.show(false)

//  +---------+----------+------+
//  |paymentId|customerId|amount|
//  +---------+----------+------+
//  |5        |105       |150   |
//  |6        |106       |450   |
//  |4        |104       |400   |
//  +---------+----------+------+

}
