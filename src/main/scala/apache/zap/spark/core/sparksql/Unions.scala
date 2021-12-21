package apache.zap.spark.core.sparksql

import apache.zap.spark.core.sparksql.WindowFunctions.Salary
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


object Unions extends App {

  import spark.implicits._
  case class Payment(payment_id: Int, customer_id: Int, amount: Int)

  val payment1 = sc.parallelize(Seq(
    (1, 101,2500), (2,102,1110), (3,103,500), (4 ,104,400), (5 ,105, 150), (6 ,106, 450)
  )).toDS().toDF("payment_id", "customer_id", "amount").as[Payment]
  payment1.printSchema()
  payment1.show()


  val payment2 = sc.parallelize(Seq(
    (101, 1001,2500), (102,1002,1110), (103,1003,500), (104 ,1004,400), (105 ,1005, 150), (106 ,1006, 450)
  )).toDS().toDF("payment_id", "customer_id", "amount").as[Payment]


  payment1.union(payment2).explain()
  //payment1.unionByName(payment2)

//  val df1 = Seq(
//    (1, 2, 3),
//    (1, 2, 3),
//    (4, 5, 6)).toDF("col0", "col1", "col2")
//  //val df2 = Seq((4, 5, 6)).toDF("col1", "col2", "col0")
//  //df1.union(df2).show
//
//  df1.show(false)
//
// df1.groupBy($"col0", $"col1", $"col2").agg($"col0", max($"col1"), $"col2").show(false)

  case class Salary(dept: String, id: Long, salary: Long)
  val employeeDS = Seq(
    Salary("personnel", 2, 3900),
    Salary("sales", 3, 4800),
    Salary("sales", 4, 4800),
    Salary("sales", 4, 4800),
    Salary("develop", 9, 4500),
    Salary("develop", 11, 5200),
    Salary("develop", 11, 5200)
  ).toDS()
  employeeDS.show(false)

  employeeDS
    .groupBy(col("dept"), col("id"), col("salary"))
    .agg($"dept".as("temp_dept"), $"id".as("temp_id"), $"salary".as("temp_salary"))
    .drop("temp_dept", "temp_id", "temp_salary")
    .show(false)
}
