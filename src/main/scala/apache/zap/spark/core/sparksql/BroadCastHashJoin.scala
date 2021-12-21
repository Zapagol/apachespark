package apache.zap.spark.core.sparksql

import org.apache.spark.sql.functions.broadcast

/**
  * Broadcast join is an important part of Spark SQL’s execution engine. When used, it performs a join on two
  * relations by first broadcasting the smaller one to all Spark executors, then evaluating the join criteria
  * with each executor’s partitions of the other relation.
  *
  * 2 types of Broadcast joins
  *   1. BroadCastHashJoin(BHJ)
  *     - Driver builds in memory hash table to distribute to executors
  *
  *   2. BroadCastNestedLoopJoin(BNLJ)
  *     - Distributes data as array to executors
  *     -  Useful for non-equi joins
  *
  * The broadcast join is controlled through spark.sql.autoBroadcastJoinThreshold configuration entry.
  * This property defines the maximum size of the table being a candidate for broadcast. If the table is much bigger
  * than this value, it won't be broadcasted.
  */
object BroadCastHashJoin extends App {

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  case class Customer(customer_id: Int, customer_name: String)
  case class Payment(payment_id: Int, customer_id: Int, amount: Int)

  case class InnerJoinedRows[A, B](left: A, right: B)

  val payment = sc.parallelize(Seq(
    (1, 101,2500), (2,102,1110), (3,103,500), (4 ,104,400), (5 ,105, 150), (6 ,106, 450)
  )).toDF("payment_id", "customer_id", "amount").as[Payment]

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

  //customer.show()

  //  +----------+----+
  //  |customerId|name|
  //  +----------+----+
  //  |       101| Jon|
  //  |       102|Aron|
  //  |       103| Sam|
  //  +----------+----+

  //get Broadcast Threshold value
  val broadCastThresholdValue = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
  println(broadCastThresholdValue) // 10485760 ~ 10 MB

  /**
    * You can notice here that, even though Dataframes are small in size sometimes spark doesn’t
    * recognize that the size of the dataframe is < 10 MB. To enforce this we can use the broadcast hint.
    */
  payment.as("p")
    .join(customer.as("c"),
      $"p.customer_id" === $"c.customer_id", "inner")
    .explain()

//  *SortMergeJoin [customer_id#9], [customer_id#25], Inner
//  :- *Sort [customer_id#9 ASC NULLS FIRST], false, 0
//    :  +- Exchange hashpartitioning(customer_id#9, 200)
//  :     +- *Project [_1#4 AS payment_id#8, _2#5 AS customer_id#9, _3#6 AS amount#10]
//  :        +- *SerializeFromObject [assertnotnull(input[0, scala.Tuple3, true])._1 AS _1#4, assertnotnull(input[0, scala.Tuple3, true])._2 AS _2#5, assertnotnull(input[0, scala.Tuple3, true])._3 AS _3#6]
//  :           +- Scan ExternalRDDScan[obj#3]
//  +- *Sort [customer_id#25 ASC NULLS FIRST], false, 0
//  +- Exchange hashpartitioning(customer_id#25, 200)
//  +- *Project [_1#22 AS customer_id#25, _2#23 AS customer_name#26]
//  +- *SerializeFromObject [assertnotnull(input[0, scala.Tuple2, true])._1 AS _1#22, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(input[0, scala.Tuple2, true])._2, true) AS _2#23]
//  +- Scan ExternalRDDScan[obj#21]

  payment.as("p")
    .join(broadcast(customer.as("c")),
      $"p.customer_id" === $"c.customer_id", "inner")
    .explain()

//  == Physical Plan ==
//    *BroadcastHashJoin [customer_id#9], [customer_id#25], Inner, BuildRight
//  :- *Project [_1#4 AS payment_id#8, _2#5 AS customer_id#9, _3#6 AS amount#10]
//  :  +- *SerializeFromObject [assertnotnull(input[0, scala.Tuple3, true])._1 AS _1#4, assertnotnull(input[0, scala.Tuple3, true])._2 AS _2#5, assertnotnull(input[0, scala.Tuple3, true])._3 AS _3#6]
//  :     +- Scan ExternalRDDScan[obj#3]
//  +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)))
//  +- *Project [_1#22 AS customer_id#25, _2#23 AS customer_name#26]
//  +- *SerializeFromObject [assertnotnull(input[0, scala.Tuple2, true])._1 AS _1#22, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(input[0, scala.Tuple2, true])._2, true) AS _2#23]
//  +- Scan ExternalRDDScan[obj#21]

}
