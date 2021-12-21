package apache.zap.spark.core.sparksql

import apache.zap.spark.core.sparksql.datasources.FileSink

//import apache.zap.spark.core.sparksql.DSJoins.{Customer, Payment, customerDS, paymentDS}

object InnerJoin extends App {

  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  case class Customer(customer_id: Int, customer_name: String)
  case class Payment(payment_id: Int, customer_id: Int, amount: Int)
  case class ID(amount: Int, customer_name: String)

  case class InnerJoinedRows[A, B](left: A, right: B)

  val payment = sc.parallelize(Seq(
    (1, 101,2500), (2,102,1110), (3,103,500), (4 ,104,400), (5 ,105, 150), (6 ,106, 450)
  )).toDS().toDF("payment_id", "customer_id", "amount").as[Payment]

  val abc = Seq(
    (1, 101,2500), (2,102,1110), (3,103,500), (4 ,104,400), (5 ,105, 150), (6 ,106, 450)
  )//.foreach(x => x)
  abc.length
//  abc.foreach(x => x.toString)
//
//    //val abc = x.to
//  //abc
//  )

  //payment.show()


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

  customer.filter($"customer_id" === 101 || $"customer_id" === 102).show(false)
  customer.filter("customer_id != 101 || customer_id != 101").show(false)
  //customer.filter("customer_id = 101").show(false)
  //customer.filter("customer_name = 'Aron'").show(false)
  //customer.filter("customer_name === 'Jon'").show(false)

  //customer.show()

//  +----------+----+
//  |customerId|name|
//  +----------+----+
//  |       101| Jon|
//  |       102|Aron|
//  |       103| Sam|
//  +----------+----+

  // Inner Join
  val innerJoinResult = payment
    .as("p")
    .join(customer.as("c"), $"p.customer_id" === $"c.customer_id")

  innerJoinResult.show()

    //  +---------+----------+------+----------+----+
    //  |paymentId|customerId|amount|customerId|name|
    //  +---------+----------+------+----------+----+
    //  |        1|       101|  2500|       101| Jon|
    //  |        3|       103|   500|       103| Sam|
    //  |        2|       102|  1110|       102|Aron|
    //  +---------+----------+------+----------+----+

    val innerJoinResult1 = payment
      .join(customer, "customer_id")

    innerJoinResult1.show()

    //  +----------+---------+------+----+
    //  |customerId|paymentId|amount|name|
    //  +----------+---------+------+----+
    //  |       101|        1|  2500| Jon|
    //  |       103|        3|   500| Sam|
    //  |       102|        2|  1110|Aron|
    //  +----------+---------+------+----+

    val innerJoinResult2 = payment
      .join(customer, Seq("customer_id"), "inner")

    innerJoinResult2.show()

    //  +----------+---------+------+----+
    //  |customerId|paymentId|amount|name|
    //  +----------+---------+------+----+
    //  |       101|        1|  2500| Jon|
    //  |       103|        3|   500| Sam|
    //  |       102|        2|  1110|Aron|
    //  +----------+---------+------+----+

    val innerJoinResult3 = payment.as("p")
      .join(customer.as("c"), $"p.customer_id" === $"c.customer_id", "inner")

    innerJoinResult3.show()
//  innerJoinResult2.write
//    //.option("header", header.toString)
//    .option("charset", "windows-31j") //IGNORED BY SPARK 2.3.1
//    .option("sep", ",")
//    //.option("escape", "\\")
//    .option("ignoreLeadingWhiteSpace", "false")
//    .option("ignoreTrailingWhiteSpace", "false")
//    //.mode(saveMode)
//    .csv("test_file")

    //  +---------+----------+------+----------+----+
    //  |paymentId|customerId|amount|customerId|name|
    //  +---------+----------+------+----------+----+
    //  |        1|       101|  2500|       101| Jon|
    //  |        3|       103|   500|       103| Sam|
    //  |        2|       102|  1110|       102|Aron|
    //  +---------+----------+------+----------+----+

    val innerJoinResult4 = payment
      .joinWith(customer,
        payment("customer_id") === customer("customer_id"),
        "inner")


    innerJoinResult4.show()

//  +--------------+-----------+
//  |            _1|         _2|
//  +--------------+-----------+
//  |[1, 101, 2500]| [101, Jon]|
//  | [3, 103, 500]| [103, Sam]|
//  |[2, 102, 1110]|[102, Aron]|
//  +--------------+-----------+

  innerJoinResult4.map{
    case (left, right) => right.customer_name + left.amount
  }.show(false)

    val combined = innerJoinResult4.map{
      case(left, right) =>ID(left.amount, right.customer_name)
    }

    //  +------------+----------+
    //  |          _1|        _2|
    //  +------------+----------+
    //  |[1,101,2500]| [101,Jon]|
    //  | [3,103,500]| [103,Sam]|
    //  |[2,102,1110]|[102,Aron]|
    //  +------------+----------+

    val innerJoinResult5 = payment
        .as("p")
        .joinWith(customer.as("c"),
          $"p.customer_id" === $"c.customer_id","inner")

    innerJoinResult5.show()

  //  +------------+----------+
  //  |          _1|        _2|
  //  +------------+----------+
  //  |[1,101,2500]| [101,Jon]|
  //  | [3,103,500]| [103,Sam]|
  //  |[2,102,1110]|[102,Aron]|
  //  +------------+----------+

    val innerJoinResult6 = payment.as("p")
      .joinWith(customer.as("c"),
        $"p.customer_id" === $"c.customer_id","inner")
      .map{
        case(left, right) =>InnerJoinedRows(left, right)
      }

    val bridged = innerJoinResult6
      .map(x => (x.left, x.right))

    bridged.show(false)

  Thread.sleep(10000000)
//  +------------+----------+
//  |_1          |_2        |
//  +------------+----------+
//  |[1,101,2500]|[101,Jon] |
//  |[3,103,500] |[103,Sam] |
//  |[2,102,1110]|[102,Aron]|
//  +------------+----------+

}
