package apache.zap.spark.core.sparksql

object RightJoin extends App{

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  case class Customer(customerId: Int, customerName: String)
  case class Payment(paymentId: Int, customerId: Int, amount: Int)
  case class RightJoinedRows[A, B](left: Option[A], right: B)

  val payment = sc.parallelize(Seq(
    (1, 101,2500), (2,102,1110), (3,103,500), (4 ,104,400), (5 ,105, 150), (6 ,106, 450)
  )).toDF("paymentId", "customerId","amount").as[Payment]

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

  val customer = sc.parallelize(Seq((101,"Jon") , (102,"Aron") ,(103,"Sam"), (120, "Robert"), (130, "Michel")))
    .toDF("customerId", "customerName").as[Customer]

  customer.show()

//  +----------+------------+
//  |customerId|customerName|
//  +----------+------------+
//  |       101|         Jon|
//  |       102|        Aron|
//  |       103|         Sam|
//  |       120|      Robert|
//  |       130|      Michel|
//  +----------+------------+

  val rightJoinDF1 = payment
    .join(customer, Seq("customerId"), "right")

  rightJoinDF1.show()

//  +----------+---------+------+------------+
//  |customerId|paymentId|amount|customerName|
//  +----------+---------+------+------------+
//  |       101|        1|  2500|         Jon|
//  |       103|        3|   500|         Sam|
//  |       120|     null|  null|      Robert|
//  |       130|     null|  null|      Michel|
//  |       102|        2|  1110|        Aron|
//  +----------+---------+------+------------+

  val rightJoinDF2 = payment
    .join(customer, Seq("customerId"), "right_outer")

  rightJoinDF2.show()

//  +----------+---------+------+------------+
//  |customerId|paymentId|amount|customerName|
//  +----------+---------+------+------------+
//  |       101|        1|  2500|         Jon|
//  |       103|        3|   500|         Sam|
//  |       120|     null|  null|      Robert|
//  |       130|     null|  null|      Michel|
//  |       102|        2|  1110|        Aron|
//  +----------+---------+------+------------+

  val rightJoinDF3 = payment
    .as("p")
    .join(customer.as("c"), $"p.customerId" === $"c.customerId", "right_outer")

  rightJoinDF3.show()

//  +---------+----------+------+----------+------------+
//  |paymentId|customerId|amount|customerId|customerName|
//  +---------+----------+------+----------+------------+
//  |        1|       101|  2500|       101|         Jon|
//  |        3|       103|   500|       103|         Sam|
//  |     null|      null|  null|       120|      Robert|
//  |     null|      null|  null|       130|      Michel|
//  |        2|       102|  1110|       102|        Aron|
//  +---------+----------+------+----------+------------+

  // joinwith
  val rightJoinDF4 = payment
    .as("p")
    .joinWith(customer.as("c"), $"p.customerId" === $"c.customerId", "right_outer")

  rightJoinDF4.show()

//  +------------+------------+
//  |          _1|          _2|
//  +------------+------------+
//  |[1,101,2500]|   [101,Jon]|
//  | [3,103,500]|   [103,Sam]|
//  |        null|[120,Robert]|
//  |        null|[130,Michel]|
//  |[2,102,1110]|  [102,Aron]|
//  +------------+------------+


  val rightOuterJoinDF1 = payment.as("p")
    .joinWith(customer.as("c"),
      $"p.customerId" === $"c.customerId","right_outer")
    .map{ case(left, right) => RightJoinedRows(Option(left), right) }

  val bridged = rightOuterJoinDF1
    .filter(_.left.isDefined)
    .map(x => (x.left.get, x.right))

  bridged.show(false)

//  +------------+----------+
//  |_1          |_2        |
//  +------------+----------+
//  |[1,101,2500]|[101,Jon] |
//  |[3,103,500] |[103,Sam] |
//  |[2,102,1110]|[102,Aron]|
//  +------------+----------+

  val bridgedCustomer = bridged.map{case(_, customer: Customer) => customer }
  bridgedCustomer.show(false)

//    +----------+-------------+
//    |customerId|customer_name|
//    +----------+-------------+
//    |101       |Jon          |
//    |103       |Sam          |
//    |102       |Aron         |
//    +----------+-------------+

  val bridgedPayment = bridged.map{case(payment: Payment, _) => payment }
  bridgedPayment.show(false)

//  +---------+----------+------+
//  |paymentId|customerId|amount|
//  +---------+----------+------+
//  |1        |101       |2500  |
//  |3        |103       |500   |
//  |2        |102       |1110  |
//  +---------+----------+------+

  val unbridged = rightOuterJoinDF1
    .filter(_.left.isEmpty)
    .map(_.right)

  unbridged.show(false)

//  +----------+------------+
//  |customerId|customerName|
//  +----------+------------+
//  |120       |Robert      |
//  |130       |Michel      |
//  +----------+------------+

}
