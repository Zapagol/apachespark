package apache.zap.spark.core.sparksql

import org.apache.spark.sql.{Column, Dataset, Encoder}

object DSJoins extends App{

  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")

  case class Customer(customer_id: Int, customer_name: String)
  case class Payment(payment_id: Int, cust_id: Int, amount: Int)
  case class LeftJoinedRows[A, B](left: A, right: Option[B])
  case class OuterJoinedRows[A, B](left: Option[A], right: Option[B])

  val paymentDS = sc.parallelize(Seq(
    (1, 101,2500), (2,102,1110), (3,103,500), (4 ,104,400), (5 ,105, 150), (6 ,106, 450)
  )).toDS().toDF("payment_id", "cust_id", "amount").as[Payment]

  paymentDS.show()

//  +----------+-----------+------+
//  |payment_id|customer_id|amount|
//  +----------+-----------+------+
//  |         1|        101|  2500|
//  |         2|        102|  1110|
//  |         3|        103|   500|
//  |         4|        104|   400|
//  |         5|        105|   150|
//  |         6|        106|   450|
//  +----------+-----------+------+

  val customerDS = sc.parallelize(Seq((101,"Jon") , (102,"Aron") ,(103,"Sam")))
    .toDF("customer_id", "customer_name").as[Customer]
  customerDS.show()

//  +-----------+-------------+
//  |customer_id|customer_name|
//  +-----------+-------------+
//  |        101|          Jon|
//  |        102|         Aron|
//  |        103|          Sam|
//  +-----------+-------------+


  // Inner Join
  val innnerJoinDF1 = paymentDS
    .join(customerDS, $"customer_id" === $"cust_id", "inner")
  innnerJoinDF1.show()

//  +----------+-------+------+-----------+-------------+
//  |payment_id|cust_id|amount|customer_id|customer_name|
//  +----------+-------+------+-----------+-------------+
//  |         1|    101|  2500|        101|          Jon|
//  |         3|    103|   500|        103|          Sam|
//  |         2|    102|  1110|        102|         Aron|
//  +----------+-------+------+-----------+-------------+

  val innnerJoinDF2 = paymentDS // Dataset[(Payment,Customer)]
    .joinWith(customerDS, $"customer_id" === $"cust_id", "inner")

  innnerJoinDF2.show()
//  +------------+----------+
//  |          _1|        _2|
//  +------------+----------+
//  |[1,101,2500]| [101,Jon]|
//  | [3,103,500]| [103,Sam]|
//  |[2,102,1110]|[102,Aron]|
//  +------------+----------+

  val modified1 = innnerJoinDF2 // Dataset[Int]
    .map{
      case (payment, customer) => payment.amount * 2
    }
  modified1.show()
//  +-----+
//  |value|
//  +-----+
//  | 5000|
//  | 1000|
//  | 2220|
//  +-----+

  val modified2 = innnerJoinDF2 // Dataset[Payment]
    .map{
    case (payment, customer) => payment.copy(amount = payment.amount * 2)
  }
  modified2.show()

//  +----------+-------+------+
//  |payment_id|cust_id|amount|
//  +----------+-------+------+
//  |         1|    101|  5000|
//  |         3|    103|  1000|
//  |         2|    102|  2220|
//  +----------+-------+------+

  val modified3 = innnerJoinDF2 // Dataset[Payment, Customer] -- modified payment amount
    .map{
      case (payment, customer) => (payment.copy(amount = payment.amount * 2), customer)
    }
  modified3.show()

//  +------------+----------+
//  |          _1|        _2|
//  +------------+----------+
//  |[1,101,5000]| [101,Jon]|
//  |[3,103,1000]| [103,Sam]|
//  |[2,102,2220]|[102,Aron]|
//  +------------+----------+


  // <--- Left Outer Join ---->
  val leftOuterJoinDF1 = paymentDS // Dataset[(Payment,Customer)]
    .joinWith(customerDS, $"customer_id" === $"cust_id", "left_outer")
    .map{
      case(left, right) =>LeftJoinedRows(left, Option(right))
    }

  val bridged = leftOuterJoinDF1
    .filter(_.right.isDefined)
    .map(x => (x.left, x.right.get))

  val unbridged = leftOuterJoinDF1
      .filter(_.right.isEmpty)
      .map(_.left)
}
